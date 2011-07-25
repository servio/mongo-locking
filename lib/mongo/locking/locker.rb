require 'mongo/locking'
require 'active_support/core_ext/numeric/time'

module Mongo
    module Locking

        # Locker is a container for isolating all the locking-related methods,
        # so we minimally impact the namespace of wherever we're mixed into.
        #
        # In addition to the Mongo-based, per-process, blocking lock mechanism
        # itself, we also employ a thread-local lock refcount to achieve
        # non-blocking behaviour when nesting lock closures.  This is useful for
        # when multiple, isolated code paths are all defensive with locks, but
        # are arbitrarily called within the same thread of execution.
        #
        # We try to limit the number of these objects, so as to minimize extra
        # cost attached to model instance hydration.  Thus the Locker is
        # attached to the model class, and maintains refcounts based on the key
        # it's configured with.

        class Locker
            include Exceptions

            attr_reader :config
            delegate    :debug, :info, :warn, :error, :fatal, :to => Locking

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

            # We increment refcounts ASARP so that any further (nested) calls
            # won't block.  But that means we have to make sure to decrement it
            # on any failure case.
            def acquire(from)
                lockable = root_for(from)
                locker   = lockable.class.locker
                scope    = locker.scope_for(lockable)
                key      = locker.key_for(lockable)
                name     = scope + "/" + key

                refcounts[key] += 1
                if refcounts[key] > 1
                    info "acquire: re-using lock for #{name}##{refcounts[key]}"
                    return lockable
                end

                target   = { :scope => scope, :key => key }
                interval = self.config[:first_retry_interval]
                retries  = 0

                debug "acquire: attempting lock of #{name}"

                begin
                    a_lock   = atomic_inc(target, { :refcount => 1 })
                    refcount = a_lock['refcount']

                    # If the refcount is 0 or somehow less than 0, after we just
                    # incremented, then retry without counting against the max.
                    if refcount < 1
                        retries -= 1
                        debug "acquire: refcount #{refcount}, unexpected state"
                        raise LockFailure
                    end

                    # Check lock expiration.
                    if a_lock.has_key?('expire_at') and a_lock['expire_at'] < Time.now

                        # If the lock is "expired". We assume the owner of the
                        # lock is "gone" without decrementing the refcount.
                        warn "acquire: #{name} lock expired"

                        # Attempt to decrement the refcount to "reverse" the
                        # damage caused by the "gone" process.  There might be
                        # more than one process trying to do this at the same
                        # time.  Therefore, we need the refcount > 1 guard.  If
                        # the lock's refcount is no longer > 1 by the time this
                        # process try to decrement, Mongo will raise a
                        # Mongo::OperationFailure.  Regardless of reason, if it
                        # fails, we fail.
                        a_lock = atomic_inc(target.merge({:refcount => {'$gt' => 1} }), { :refcount => -1 }) rescue nil

                        unless a_lock
                            # We lost the race to "reverse" the damage.  Someone
                            # else has the lock now.  We will retry.
                            raise LockFailure
                        end

                        # We have won the race to "reverse" the damage but we
                        # may have not "reversed" enough of the damage.
                        # Consider the case that the expired lock has a large
                        # refcount - we still need to check refcount to make
                        # sure that we are have acquired the lock.
                        refcount = a_lock['refcount']

                        # The rest of the expired_lock handling logic coincides
                        # with normal lock logic.
                    end

                    # If recount is greater than 1, we lost the race.  Decrement
                    # and try again.
                    if refcount > 1
                        atomic_inc(target, { :refcount => -1 })
                        debug "acquire: refcount #{refcount}, race lost"
                        raise LockFailure
                    end

                    # If refcount == 1, we have the lock and thus renew
                    # its expire_at.
                    #
                    # NOTE: This expire_at renewal only happens when a process
                    # acquires the lock for the first time.  Subsequent lock
                    # reuse will NOT renew expire_at.  This assumes that all
                    # legitimate operations should complete within
                    # config[:max_lifetime] time limit.

                    # Mongo only works with UTC time
                    atomic_update(target, {'expire_at' => self.config[:max_lifetime].from_now.utc})

                rescue LockFailure => e
                    retries += 1
                    if retries >= self.config[:max_retries]
                        refcounts[key] -= 1
                        raise LockTimeout, "unable to acquire lock #{name}"
                    end

                    warn "acquire: #{name} refcount #{refcount}, retry #{retries} for lock"

                    sleep(interval.to_f)
                    interval = [self.config[:max_retry_interval].to_f, interval * 2].min
                    retry

                rescue => e
                    refcounts[key] -= 1

                    log_exception(e)
                    raise LockFailure, "unable to acquire lock #{name}"
                end

                info "acquire: #{name} locked (#{refcount})"

                return lockable
            end

            def release(from)
                lockable = root_for(from)
                locker   = lockable.class.locker
                key      = locker.key_for(lockable)
                scope    = locker.scope_for(lockable)
                name     = scope + "/" + key

                refcounts[key] -= 1
                if refcounts[key] > 0
                    info "release: re-using lock for #{name}##{refcounts[key]}"
                    return true
                end

                target = { :scope => scope, :key => key }

                refcount = atomic_inc(target, { :refcount => -1 })['refcount']

                info "release: #{name} unlocked (#{refcount})"

                # If the refcount is at zero, nuke it out of the table.
                #
                # NOTE: If the following delete fails (e.g. something else
                # incremented it before we tried to delete it), it will raise:
                #
                #    <Mongo::OperationFailure: Database command 'findandmodify'
                #       failed: {"errmsg"=>"No matching object found", "ok"=>0.0}>
                #
                # This is normal for a concurrent system.
                #
                # Since a lock with refcount 0 does not impact lock functionality,
                # we can also ignore any other exceptions during lock deletion.
                #
                # We use 'rescue nil' to ignore all exceptions.
                if refcount == 0
                    if hash = atomic_delete(target.merge({ :refcount => 0 })) rescue nil
                        debug "release: lock #{name} no longer needed, deleted"
                    end

                    # Nuke the key from our instance refcounts so we don't
                    # balloon during long-lived processes.
                    refcounts.delete(key)
                end

            rescue => e
                log_exception(e)
                raise LockFailure, "unable to release lock #{name}"
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

            # Separated out in case you want to monkeypatch it into oblivion.
            def log_exception(e)
                error e.inspect + " " + e.backtrace[0..4].inspect
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
                    :update => {'$inc' => update_hash.dup},
                })
            end

            def atomic_update(search_hash, update_hash)
                return Locking.collection.find_and_modify({
                    :new    => true,    # Return the updated document
                    :upsert => true,    # Update if document exists, insert if not
                    :query  => search_hash.merge({'$atomic' => 1}),
                    :update => {'$set' => update_hash.dup},
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

