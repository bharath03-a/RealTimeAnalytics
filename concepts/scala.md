## Scala Concepts I learned along the way and it's best practices

### Classes

- A blueprint for creating objects.
- Can have **fields** and **methods** and support **inheritance**.
- Mutable by default unless using `val` for fields.

```scala
class Person(val name: String, var age: Int) {
  def greet(): String = s"Hi, my name is $name"
}
```

- Prefer immutable fields (val) when possible.
- Keep classes focused on a single responsibility.
- Use constructor parameters to enforce required fields.

### Case Classes

- Special class for **immutable data structures**.
- Auto-generates: `equals`, `hasCode`, `toString`, `copy` and `apply`.
- Supports pattern matching

```scala
case class InteractionEvent(userId: String, eventType: String, reelId: String)
val event = InteractionEvent("u1", "like", "r1")
```

- Use case classes for data models (like messages, events, configs).
- Avoid using case classes for mutable state.
- Combine with **companion objects for factory methods**.

```scala
// Case class with primary constructor
case class Person(name: String, age: Int)

// Companion object with custom factory methods
object Person {
  // Factory method with default age
  def createWithDefaultAge(name: String): Person = Person(name, 0)

  // Factory method with age validation
  def createAdult(name: String, age: Int): Option[Person] =
    if (age >= 18) Some(Person(name, age)) else None
}

// Using the default case class apply method
val p1 = Person("Alice", 25)

// Using custom factory methods
val p2 = Person.createWithDefaultAge("Bob")
val p3 = Person.createAdult("Charlie", 17) // returns None
val p4 = Person.createAdult("David", 20)   // returns Some(Person("David", 20))

println(p1) // Output: Person(Alice,25)
println(p2) // Output: Person(Bob,0)
println(p3) // Output: None
println(p4) // Output: Some(Person(David,20))
```

### Object (Singletons)

- A singleton instance (one per JVM)
- Can contain methods, constants, companion object factory methods.

```scala
object Utils {
  def add(a: Int, b: Int): Int = a + b
}
println(Utils.add(2,3))
```

- Use objects for utility functions, constants, or single-instance services.
- Pair an object with a case class as a companion object for factory methods.

### Trait

- Similar to an interface, but can include concrete methods.
- Supports multiple inheritance (mixins).

```scala
trait Animal {
  def makeSound(): String
  def eat(): String = "Eating"
}

class Dog extends Animal {
  def makeSound(): String = "Woof!"
}
```

- Use traits for shared behavior across classes.
- Keep traits focused on a single responsibility.
- Prefer composition over inheritance when combining multiple traits.

### System Design Patterns

#### Singleton Pattern

Implemented using object.

#### Factory Pattern

Implemented via companion objects with factory methods.

```scala
object PipelineArgs {
  def fromArgs(args: Args): PipelineArgs = ...
}
```

#### Builder Pattern

Can be implemented via case class copy for immutability.

```scala
val newArgs = oldArgs.copy(threshold = 5)
```