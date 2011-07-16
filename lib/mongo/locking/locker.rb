# Locker is a container for locking related commands, so we minimally impact the
# model's namespace.
#
# In addition to the Mongo-based, per-process, blocking lock mechanism itself,
# we also employ a thread-local lock refcount to achieve non-blocking behaviour
# when nesting lock closures.  This is useful for when multiple, isolated code
# paths are all defensive with locks, but are arbitrarily called within the same
# thread of execution.
#
# We try to limit the number of these objects, so as to minimze extra cost
# attached to model instance hydration.  Thus the Locker is attached to the
# model class, and maintains refcounts based on the key it's configured with.

require 'mongo/locking'
require 'active_support/core_ext/numeric/time'

module Mongo
    module Locking

        class Locker
            include Exceptions

            attr_reader :config

            DEFAULT_OPTIONS = {
                :max_retries          => 5,
                :first_retry_interval => 0.2.seconds,
                :max_retry_interval   => 5.seconds,
                :max_lifetime         => 10.minutes,
            }

            def initialize(opts = {})
                @config       = DEFAULT_OPTIONS.merge(opts)
                @refcount_key = "mongo_locking_refcounts_#{@config[:class_name]}"
            end

            def refcounts
                Thread.current[@refcount_key] ||= Hash.new(0)
            end

            # refcounts[] is about enabling non-blocking behaviour when the
            # process has already acquired the lock (e.g. controller locks
            # Order, calls model save method that also locks Order).  In a way,
            # it's like an IdentityMap, mapping instance keys to reference
            # counts (another reason it lives on the class and not the
            # instance).
            #
            # We increment it immediately so that any further (nested) calls
            # won't block, but that means we have to make sure to decrement it
            # on every failure case.

            def acquire(from)
                lockable = root_for(from)
                locker   = lockable.class.locker

                scope = locker.scope_for(lockable)
                key   = locker.key_for(lockable)
                name  = scope + "/" + key

                refcounts[key] += 1
                if refcounts[key] > 1
                    Locking.info "acquire: re-using lock for #{name}##{refcounts[key]}"
                    return lockable
                end

                target   = { :scope => scope, :key => key }
                interval = self.config[:first_retry_interval]
                retries  = 0

                Locking.debug "acquire: attempting lock of #{name}"

                begin
                    a_lock   = atomic_inc(target, { :refcount => 1 })
                    refcount = a_lock['refcount']

                    # FIXME: If the lock is "expired" then we just inherit the
                    # lock and assume the user of the lock is "gone", right?  Do
                    # we need to do some decr/incr?  Is this correct?
                    if a_lock['expire_at'] < Time.now
                        refcount = 1
                    end

                    # If the refcount is 0 or somehow less than 0, after we just
                    # incremented, then retry without counting against the max.
                    if refcount < 1
                        retries -= 1
                        Locking.debug "acquire: refcount #{refcount}, unexpected state"
                        raise LockFailure
                    end

                    # If recount is greater than 1, we lost the race.  Decrement
                    # and try again.
                    if refcount > 1
                        atomic_inc(target, { :refcount => -1 })
                        Locking.debug "acquire: refcount #{refcount}, race lost"
                        raise LockFailure
                    end

                rescue LockFailure => e
                    retries += 1
                    if retries >= self.config[:max_retries]
                        refcounts[key] -= 1
                        raise LockTimeout, "unable to acquire lock #{name}"
                    end

                    Locking.warn "acquire: #{name} refcount #{refcount}, retry #{retries} for lock"

                    sleep(interval.to_f)
                    interval = [self.config[:max_retry_interval].to_f, interval * 2].min
                    retry

                rescue => e
                    refcounts[key] -= 1

                    log_exception(e)
                    raise LockFailure, "unable to acquire lock #{name}"
                end

                Locking.info "acquire: #{name} locked (#{refcount})"

                return lockable
            end

            def release(from)
                lockable = root_for(from)
                locker   = lockable.class.locker

                key   = locker.key_for(lockable)
                scope = locker.scope_for(lockable)
                name  = scope + "/" + key

                refcounts[key] -= 1
                if refcounts[key] > 0
                    Locking.info "release: re-using lock for #{name}##{refcounts[key]}"
                    return true
                end

                target = { :scope => scope, :key => key }

                begin
                    refcount = atomic_inc(target, { :refcount => -1 })['refcount']

                    Locking.info "release: #{name} unlocked (#{refcount})"

                    # If the refcount is at zero, nuke it out of the table.
                    #
                    # FIXME: If the following delete fails (e.g. something else
                    # incremented it before we tried to delete it), it will
                    # raise:
                    #
                    #    <Mongo::OperationFailure: Database command 'findandmodify'
                    #       failed: {"errmsg"=>"No matching object found", "ok"=>0.0}>
                    #
                    # Need to see if there's a way to report the error
                    # informationally instead of exceptionally.  'rescue nil'
                    # hack in place until something more correct is put in.
                    if refcount == 0
                        unless hash = atomic_delete(target.merge({ :refcount => 0 })) rescue nil
                            Locking.debug "release: lock #{name} no longer needed, deleted"
                        end
                    end

                rescue => e
                    log_exception(e)
                    raise LockFailure, "unable to release lock #{name}"
                end
            end

            def root_for(from)
                lockable = from
                visited  = Set.new([lockable.class])

                while parent = lockable.class.locker.parent_for(lockable)
                    lockable = parent

                    if visited.include? lockable.class
                        raise CircularLock, "already visited #{lockable.class} (#{visited.inspect})"
                    end

                    visited << lockable.class
                end

                raise InvalidConfig, "root #{lockable.inspect} is not lockable" unless lockable.class.locker.is_root?

                return lockable
            end

            def key_for(lockable)
                return case key = self.config[:key]
                    when Proc   then key.call(self).to_s
                    when Symbol then lockable.send(key).to_s
                    else raise InvalidConfig, "unknown key type #{key.inspect}"
                end
            end

            def scope_for(lockable)
                return case scope = self.config[:scope]
                    when Proc   then scope.call(lockable).to_s
                    when Symbol then scope.to_s
                    when String then scope
                    else raise InvalidConfig, "unknown scope type #{scope.inspect}"
                end
            end

            def parent_for(lockable)
                return case parent = self.config[:parent]
                   when Proc     then parent.call(lockable)
                   when Symbol   then lockable.send(parent)
                   when NilClass then nil
                   else raise InvalidConfig, "unknown parent type #{parent.inspect}"
                end
            end

            def is_root?
                self.config[:parent].nil?
            end

            private

            def log_exception(e)
                Locking.error e.inspect + " " + e.backtrace[0..4].inspect
            end

            # Notes:
            #
            #   - search_hash contains mapping of search_key => search_value
            #   - search_key is the key used to uniquely identify the document
            #     to update
            #   - search_key MUST be the sharding key if we do sharding
            #     later. This ensures that "$atomic" still meaningful
            #
            # Mongo:
            #
            #   - $atomic actually means "write isolation". '$atomic'=>1 means
            #     we are the only writer to this document.
            #   - '$inc' means "atomic increment"
            #   - set expire_at for future async/lazy lock reaping

            def atomic_inc(search_hash, update_hash)
                return Locking.collection.find_and_modify({
                    :new    => true,    # Return the updated document
                    :upsert => true,    # Update if document exists, insert if not
                    :query  => search_hash.merge({'$atomic' => 1}),
                    :update => {
                        '$inc' => update_hash.dup,
                        '$set' => { 'expire_at' => self.config[:max_lifetime].from_now }
                    },
                })
            end

            def atomic_delete(search_hash)
                return Locking.collection.find_and_modify({
                    :query  => search_hash.merge({'$atomic' => 1}),
                    :remove => true,
                })
            end

        end # Locker
    end # Locking
end # Mongo
