##
## Usages
##
#
#   Order.lockable!
#   Order.lockable! :key => :id
#   Order.lockable! :key => proc { |me| SHA1.hexdigest(me.balls) }
#   Order.lockable! :scope => "OtherClass"
#
#   OrderItem.locked_by! { |me| me.order }
#   OrderItem.locked_by! :parent => proc { |me| me.order }
#   OrderItem.locked_by! :order
#   OrderItem.locked_by! :parent => :order
#
##
## Useful tests
##
#
#   Pn == process N
#   Order.id == 1
#   OrderItem.id == 1, OrderItem.order_id = 1
#
# - General race, same object
#
#   P1: Order.first.lock { debugger }  # gets and holds lock
#   P2: Order.first.lock { puts "hi" } # retries acquire, fails
#
# - General race, locked root, attempt to lock from child
#
#   P1: Order.first.lock { debugger }  # gets and holds lock
#   P2: OrderItem.first.lock { puts "hi" } # retries acquire, fails
#
# - General race, locked root from child, attempt to lock from child
#
#   P1: OrderItem.first.lock { debugger }  # gets and holds lock
#   P2: OrderItem.first.lock { puts "hi" } # retries acquire, fails
#
# - Nested lock acquisition
#
#   P1: Order.first.lock { puts "1"; Order.first.lock { puts "2" } }
#   # should see 1 and 2
#
