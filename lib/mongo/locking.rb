##
## Mongo Locking Library
##
#
# Despite the Locker being placed on the model classes, the dependency graph is
# actually instance-based, thus the graph can only be resolved during runtime.
#
# Locking takes an instance to start with, optionally walks the graph through
# parent "pointers" (procs whose arbitrary function produces the parent
# lockable), then does the atomic incr/decr dance with the root lockable's key.
#
# TODO: More docs.  TBC.

require 'mongo'
require 'active_support/core_ext/module/delegation'

module Mongo
    module Locking
        extend self

        module Exceptions
            class Error < RuntimeError;  end
            class ArgumentError < Error; end # bad param
            class InvalidConfig < Error; end # something bogus in config[]
            class CircularLock  < Error; end # circular lock reference
            class LockTimeout   < Error; end # lock failed, retries unsuccessful
            class LockFailure   < Error; end # lock failed, something bad happened
        end
        include Exceptions

        attr_accessor :logger, :collection
        delegate      :debug, :info, :warn, :error, :fatal, :to => :logger, :allow_nil => true

        def included(klass)
            klass.send(:include, ModelMethods)
        end

        ##
        ## Configuration
        ##

        def configure(opts = {})
            opts.each { |k, v| send("#{k}=", v) }
            return self
        end

        def collection=(new)
            @collection = new
            ensure_indices if @collection.kind_of? Mongo::Collection
            return @collection
        end

        # Allow for a proc to be initially set as the collection, in case of
        # delayed loading/configuration/whatever.  It'll only be materialized
        # when first accessed, which is presumably either when ensure_indices is
        # called imperatively, or more likely upon the first lock call.
        def collection
            if @collection.kind_of? Proc
                @collection = @collection.call
                ensure_indices if @collection.kind_of? Mongo::Collection
            end
            return @collection
        end

        ##
        ## Document Indices
        ##
        #
        # A Mongoid model would look like:
        #   field :scope,     :type => String
        #   field :key,       :type => String
        #   field :refcount,  :type => Integer, :default => 0
        #   field :expire_at, :type => DateTime
        #   index [ [ :scope, Mongo::ASCENDING ], [ :key, Mongo::ASCENDING ] ], :unique => true
        #   index :refcount
        #   index :expire_at

        LOCK_INDICES = {
            [["scope", Mongo::ASCENDING], ["key", Mongo::ASCENDING]] => { :unique => true,  :background => true },
            [["refcount", Mongo::ASCENDING]]                         => { :unique => false, :background => true },
            [["expire_at", Mongo::ASCENDING]]                        => { :unique => false, :background => true },
        }

        def ensure_indices
            LOCK_INDICES.each do |spec, opts|
                @collection.ensure_index(spec, opts)
            end
        end

    end # Locking
end # Mongo

require 'mongo/locking/locker'
require 'mongo/locking/model_methods'

