### Summary

Mongo::Locking is a library that effectively enables cross-process blocking
mutexes, using simple but flexible primitives to express an arbitrary graph of
lock dependencies between class instances.

#### Background

Consider the following fairly common scenario:

Given an object graph 1 Order -> N OrderItems -> 1 JobFlow -> N Jobs, a
collection of disparate systems that operate on portions of the graph
asynchronously.

If you were using a Document-oriented (e.g. Mongo) data model, you might
represent the object graph as a bunch of nested objects, rooted in an Order.  In
an RDBMS, you usually have a collection of tables with foreign key relationships
between them.

In any case, you need to enforce some notion of data integrity as portions of
the graph mutate.  How does one normally enforce integrity in concurrent access
scenarios?

#### RDBMS

In the RDBMS world, you've got a couple options:

1. `SELECT .. FOR UPDATE` or equivalent.

    Depending on the underlying storage engine, this will write lock at minimum the
    given row, and in most modern RDBMS', a cluster of rows around the row you're
    trying to "protect".  This approach tends to require breaking out of the ORM
    with custom SQL, and carries with it all sorts of unintended/unexpected
    performance/synchronization/deadlock pitfalls.  It really starts to break down
    when there is more than one object that needs to be "locked", or when crossing
    database boundaries.

2. Rely on the ACID properties of SQL92 transactions to enforce data integrity.

    Given 2 or more competing, disparate processes running asynchronously, accessing
    the same resources.  Both enter into transactions, possibly access overlapping
    resources, one wins and the other (eventually) fails after attempting to commit.

    Practically, what does the erroring code do?  Does it retry?  Was it written in
    a way that even makes a retry possible?  Is the context so consistently atomic
    and stateless that it could blindly do so?  Does it just bail and fail?  (Yes,
    most of the time.)  What if it was acting on an asynchronous imperative across a
    message bus?  Must this condition be detected by additional code, and the
    imperative replayed by some other code somewhere else?  Wouldn't it be
    conditional on

    OR, both enter into transactions, but the relationship between resources is not
    expressed in terms of RDBMS constraints (the common case), so the RDBMS has no
    idea that integrity has been violated.

    Transactions may sound like a panacea at first, but in real-world complex
    systems, their edge conditions often bear a greater cost and complexity than the
    problems they're being used to solve.

#### NoSQL

In the NoSQL world, you don't have as many options.  A lot get fooled by the
false perception that logically embedded objects are somehow protected by that
nesting (no, they aren't).  Some engines may even provide locking or
pseudo-transactional primitive(s), but most often the same pitfalls of SQL92
transactions will apply, especially in scale (distributed, partitioned)
environments.

### A Solution

However, when certain requirements are satisfied, one mechanism can
substantively bridge the gap: atomic increment/decrement.  Anything that
implements it can build a mutual-exclusion/locking system.  So that's what this
library does.

Qualities:

- must be reasonably "fast" (hash-time lookup)
- must be non-blocking ("retry-able")
- must be recoverable (expiration of dead/stale locks)
- must be able to be monitored / administered

Behaviour:

- blocks for a configurable duration when acquiring a lock across execution threads
- *doesn't* block when (re-)acquiring a lock within the same thread of execution


.. TBC ..



### Usage

While Mongo::Locking depends on Mongo, it can be applied to just about any ORM
or class structure.  All locks have a namespace (scope) and a key (some
instance-related value), classes can depend on others for their locks, and the
dependency graph is resolved at invocation-time.

Consider this simplified example of using it with DataMapper:

```ruby
class Order
    include ::DataMapper::Resource
    include ::Mongo::Locking

    has n, :order_items

    lockable!
end

class OrderItem
    include ::DataMapper::Resource
    include ::Mongo::Locking

    belongs_to :order
    has 1, :job_flow

    # invokes method to get "parent" lockable
    locked_by! :order
end

class JobFlow
    include ::DataMapper::Resource
    include ::Mongo::Locking

    belongs_to :order_item

    # also takes a closure, yielding some abitrary "parent" lockable
    locked_by! { |me| me.order_item }
end
```

Other (simplified) invocations:

```ruby
Order.lockable! :key => :id
Order.lockable! :scope => "OtherClass"
Order.lockable! :key => proc { |me| SHA1.hexdigest(me.balls) }

OrderItem.locked_by! { |me| me.order }
OrderItem.locked_by! :parent => proc { |me| me.order }
OrderItem.locked_by! :order
OrderItem.locked_by! :parent => :order
```

### Testing

Well, testing concurrency, especially in Ruby, is "difficult".  For now, here's
some irb-level conceptual tests that this library works with:

Given:

    Pn == process N
    Order.id == 1
    OrderItem.id == 1, OrderItem.order_id = 1

1. General race, same object

        P1: Order.first.lock { debugger }  # gets and holds lock
        P2: Order.first.lock { puts "hi" } # retries acquire, fails

2. General race, locked root, attempt to lock from child

        P1: Order.first.lock { debugger }  # gets and holds lock
        P2: OrderItem.first.lock { puts "hi" } # retries acquire, fails

3. General race, locked root from child, attempt to lock from child

        P1: OrderItem.first.lock { debugger }  # gets and holds lock
        P2: OrderItem.first.lock { puts "hi" } # retries acquire, fails

4. Nested lock acquisition

        P1: Order.first.lock { puts "1"; Order.first.lock { puts "2" } }
        # should see 1 and 2
