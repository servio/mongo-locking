require 'active_support/concern'
require 'active_support/core_ext/array/extract_options'
require 'mongo/locking'

module Mongo
    module Locking

        module ModelMethods
            extend ::ActiveSupport::Concern

            module ClassMethods

                # Options:
                #
                #   :key   => Symbol (sent to self), Proc (passed self) -> result.to_s
                #   :scope => String, Symbol (default self.name)
                #
                #   :max_retries          => default 5
                #   :first_retry_interval => default 0.2.seconds
                #   :max_retry_interval   => default 5.seconds
                #   :max_lifetime         => default 1.minute
                #
                def lockable!(opts = {})
                    key   = opts[:key]   ||= :id
                    scope = opts[:scope] ||= self.name

                    raise ArgumentError, "locker key must be a Proc or Symbol" unless [Proc, Symbol].include?(key.class)
                    raise ArgumentError, "locker scope must be a Proc, Symbol or String" unless [Proc, Symbol, String].include?(scope.class)

                    opts[:class_name] = self.name
                    @locker = Locker.new(opts)

                    return self
                end

                # Options:
                #
                #   :parent => instance of lockable parent
                #
                def locked_by!(*args, &parent)
                    opts   = args.extract_options!
                    parent = opts[:parent] ||= parent || args.first

                    raise ArgumentError, "parent reference must be a Proc or Symbol" unless [Proc, Symbol].include?(parent.class)

                    opts[:class_name] = self.name

                    @locker = Locker.new(opts)

                    return self
                end

                # No-frills class-inheritable locker reference
                def locker
                    @locker ||= (superclass.locker if superclass.respond_to?(:locker))
                end

            end # ClassMethods

            module InstanceMethods

                # Main closure-based lock method.  opts remains present for
                # future utility.
                def lock(opts = {})
                    raise ArgumentError, "#{self.class.name}#lock requires a block" unless block_given?

                    # Look for the top level lockable
                    lockable = self.class.locker.root_for(self)

                    # Try to acquire the lock. If succeeds, "locked" will be set
                    locked = lockable.class.locker.acquire(lockable)

                    return yield

                rescue => e
                    Locking.error "#{self.class.name}#lock failed"
                    raise e

                ensure
                    # Only unlock if "locked" was set.  We're using this to
                    # distinguish between an exception from the yield vs. an
                    # exception from our own locking code.  Doing it in an
                    # ensure block makes us defensible against a return from
                    # within the closure, too.
                    #
                    # Calling lockable's locker instead of self potentially
                    # saves us the cost of "find root lockable" that locker
                    # would perform.
                    lockable.class.locker.release(lockable) if locked
                end

                # Return true if operating within an open acquired lock.
                def have_lock?
                    lockable = self.class.locker.root_for(self)
                    locker = lockable.class.locker
                    key = locker.key_for(lockable)
                    return locker.refcounts[key] > 0
                end

            end # InstanceMethods

        end # ModelMethods
    end # Locking
end # Mongo

