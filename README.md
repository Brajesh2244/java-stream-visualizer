# 🌊 Java Stream API Interactive Visualizer

An interactive, educational visualizer designed to make **Source → Intermediate Operations → Terminal Operations** intuitive and crystal-clear for beginners and Java interview preparation.

---

## 🚀 How to Run

1. Open [`index.html`](index.html) in any modern web browser (Google Chrome, Firefox, Microsoft Edge, Safari).
2. No dependencies, no npm install, no backend server required! It is 100% self-contained vanilla HTML5, CSS3, and JavaScript.

---

## 🌟 Key Features

### 1. 🛠️ "Build Your Stream" (Pipeline Simulator)
- **Source Selection**: Switch between `List<Integer>`, mixed arrays, strings, primitive arrays, or type your own custom comma-separated numbers.
- **Dynamic Intermediate Chain**: Add, remove, and configure operations (`filter`, `map`, `limit`, `distinct`, `sorted`, `skip`, `takeWhile`, `dropWhile`, `peek`).
- **Terminal Operation**: Choose from `toList()`, `count()`, `findFirst()`, `min()`, `max()`, `reduce()`, `anyMatch()`, and more.
- **Step-by-Step Flow Animation**: Press **Run Pipeline** or use **Next Step / Previous Step** controls to animate actual data badges moving through each pipeline stage.
- **Return Type Progression Bar**: Visually displays type transitions:
  `List<Integer>` → `Stream<Integer>` → `Stream<Integer>` → `List<Integer>`
- **Live Java Code Generator**: Produces ready-to-run Java code with a one-click **Copy Code** button.
- **Curated Presets**: Quickly load real-world scenarios (e.g., *Even Numbers Doubled & Sorted*, *Filter Long Words & Count*, *First Number > 20*).

---

### 2. 🧪 Operations Explorer (Deep Dive)
- Clickable cards for **11 Intermediate Operations** and **14 Terminal Operations**.
- Detailed inspector showing:
  - **"Why?" Explanation**: Plain English explanation of the operation's purpose.
  - **Java Syntax**: Exact method signature and lambda expression.
  - **Input Data**: Visually presented as discrete data boxes `[10] [20] [30]`.
  - **Step-by-Step Transformation Trace**: Explains why each item was kept, removed, or transformed.
  - **Output Stream / Final Result**: Output data boxes with color-coded status badges.
  - **Return Type**: Clearly indicates `Stream<T>`, `Optional<T>`, `List<T>`, `long`, `boolean`, etc.

---

### 3. ⚡ Lazy Evaluation Lab
- Demonstrates how Streams execute **pull-based, element-by-element**, rather than running eager collection loops.
- Step-by-step element evaluation simulation:
  - `10` → fails filter condition → discarded (map is never invoked!).
  - `20` → fails filter condition → discarded.
  - `30` → passes filter → multiplied by 2 (`60`) → terminal `findFirst()` completes immediately!
  - `40` and `50` are completely skipped via **short-circuiting**.
- **Interactive Terminal Toggle**: Turn off the terminal operation to see that without a terminal operation, **zero operations are executed**.

---

### 4. 📦 Stream Sources (10 Ways to Create Streams)
Visual data box breakdown and Java syntax for:
1. `List<Integer>` (`list.stream()`)
2. `Set<String>` (`set.stream()`)
3. `Object Array` (`Arrays.stream(array)`)
4. `int[]` (`Arrays.stream(ints)` → `IntStream`)
5. `long[]` (`Arrays.stream(longs)` → `LongStream`)
6. `double[]` (`Arrays.stream(doubles)` → `DoubleStream`)
7. `Map keys` (`map.keySet().stream()`)
8. `Map values` (`map.values().stream()`)
9. `Map entries` (`map.entrySet().stream()`)
10. `Stream.of(...)` (`Stream.of("Alpha", "Beta")`)

*Includes a "Load into Pipeline Builder" button directly on each source card!*

---

### 5. 🎯 "Remember This" Cheat Sheet & Interview Pitfalls
- **The Golden Stream Rule**:
  - 🟢 **Intermediate Operations**: Return `Stream<T>` (continues the pipeline).
  - 🟠 **Terminal Operations**: Consume the stream, trigger execution, and produce a final result (stream closes).
- **Return Type Matrix Table**: Quick lookup of categories, return types, and short-circuiting behaviors.
- **Top 4 Java Stream Interview Traps**:
  1. *Stream Reuse Exception* (`IllegalStateException: stream has already been operated upon or closed`).
  2. *Order of Operations* (filter before map).
  3. *Infinite Streams without limit()*.
  4. *forEach() with shared mutable state*.

---

## 🎨 Visual Rules

| Color Code | Meaning |
| :--- | :--- |
| 🟢 **Green** | **Intermediate Operation** &bull; Returns `Stream<T>` &bull; Stream continues |
| 🟠 **Orange** | **Terminal Operation** &bull; Consumes Stream &bull; Stream ends |
| 🟣 **Purple** | **Source** &bull; Original data container |
| 🔵 **Blue / Cyan** | **Data Box / Final Result** &bull; Active evaluation elements |
