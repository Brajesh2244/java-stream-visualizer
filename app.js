/**
 * Java Stream API Playground - Complete Interactive Engine
 * Powers Pipeline Builder, Real Data Token Animations, Lazy Evaluation Lab,
 * Loop vs Stream Comparison, Interview Mode Quiz, and Code Generation.
 */

// =============================================================================
// 1. DATA CATALOGS: SOURCES (13 SOURCES)
// =============================================================================

const SOURCES_CATALOG = [
  {
    id: "list-int",
    name: "List<Integer>",
    category: "Collection",
    syntax: "List<Integer> list = Arrays.asList(10, 20, 30, 40, 50);\nStream<Integer> stream = list.stream();",
    returnType: "Stream<Integer>",
    items: [10, 20, 30, 40, 50],
    description: "Standard Java collection stream via .stream(). Wraps boxed Integer objects."
  },
  {
    id: "set-string",
    name: "Set<String>",
    category: "Collection",
    syntax: 'Set<String> set = Set.of("Apple", "Banana", "Cherry");\nStream<String> stream = set.stream();',
    returnType: "Stream<String>",
    items: ["Apple", "Banana", "Cherry"],
    description: "Unordered stream with guaranteed unique elements."
  },
  {
    id: "array-obj",
    name: "Object Array (String[])",
    category: "Array",
    syntax: 'String[] langs = {"Java", "Python", "Rust"};\nStream<String> stream = Arrays.stream(langs);',
    returnType: "Stream<String>",
    items: ["Java", "Python", "Rust"],
    description: "Object array converted using Arrays.stream(array) or Stream.of(array)."
  },
  {
    id: "array-int",
    name: "int[] (Primitive Stream)",
    category: "Primitive Stream",
    syntax: "int[] nums = {5, 12, 18, 24, 30};\nIntStream stream = Arrays.stream(nums);",
    returnType: "IntStream",
    items: [5, 12, 18, 24, 30],
    description: "Specialized IntStream avoiding autoboxing and unboxing overhead."
  },
  {
    id: "array-long",
    name: "long[] (Primitive Stream)",
    category: "Primitive Stream",
    syntax: "long[] timestamps = {100L, 250L, 500L, 1000L};\nLongStream stream = Arrays.stream(timestamps);",
    returnType: "LongStream",
    items: ["100L", "250L", "500L", "1000L"],
    description: "Specialized LongStream for 64-bit numerical computations."
  },
  {
    id: "array-double",
    name: "double[] (Primitive Stream)",
    category: "Primitive Stream",
    syntax: "double[] rates = {1.5, 2.8, 3.2, 4.9};\nDoubleStream stream = Arrays.stream(rates);",
    returnType: "DoubleStream",
    items: [1.5, 2.8, 3.2, 4.9],
    description: "Specialized DoubleStream for high-performance floating-point math."
  },
  {
    id: "map-keys",
    name: "Map Keys (map.keySet())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 1, "B", 2, "C", 3);\nStream<String> stream = map.keySet().stream();',
    returnType: "Stream<String>",
    items: ["Key:A", "Key:B", "Key:C"],
    description: "Streams through keys of a Map using .keySet().stream()."
  },
  {
    id: "map-values",
    name: "Map Values (map.values())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 10, "B", 20, "C", 30);\nStream<Integer> stream = map.values().stream();',
    returnType: "Stream<Integer>",
    items: [10, 20, 30],
    description: "Streams values of a Map using .values().stream()."
  },
  {
    id: "map-entries",
    name: "Map Entries (map.entrySet())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 1, "B", 2);\nStream<Map.Entry<String, Integer>> stream = map.entrySet().stream();',
    returnType: "Stream<Map.Entry<K, V>>",
    items: ["A=1", "B=2"],
    description: "Streams key-value pairs as Map.Entry objects. Best when both key and value are needed."
  },
  {
    id: "stream-of",
    name: "Stream.of(...)",
    category: "Factory Method",
    syntax: 'Stream<String> stream = Stream.of("Alpha", "Beta", "Gamma");',
    returnType: "Stream<String>",
    items: ["Alpha", "Beta", "Gamma"],
    description: "Direct static factory method to construct a stream from explicit values."
  },
  {
    id: "files-lines",
    name: "Files.lines(Path)",
    category: "I/O Stream",
    syntax: 'try (Stream<String> lines = Files.lines(Path.of("data.txt"))) {\n    lines.forEach(System.out::println);\n}',
    returnType: "Stream<String>",
    items: ["line 1: log", "line 2: info", "line 3: warn"],
    description: "Lazy file reading line-by-line; must be used with try-with-resources to avoid file descriptor leaks."
  },
  {
    id: "stream-iterate",
    name: "Stream.iterate() (Infinite)",
    category: "Infinite Generator",
    syntax: "Stream<Integer> stream = Stream.iterate(0, n -> n + 2).limit(5);",
    returnType: "Stream<Integer>",
    items: [0, 2, 4, 6, 8],
    description: "Produces an infinite sequential ordered stream; MUST be bounded with limit() or takeWhile()."
  },
  {
    id: "stream-generate",
    name: "Stream.generate() (Infinite)",
    category: "Infinite Generator",
    syntax: "Stream<Double> stream = Stream.generate(Math::random).limit(4);",
    returnType: "Stream<Double>",
    items: [0.12, 0.85, 0.43, 0.97],
    description: "Produces an infinite unordered stream where each element is generated by a Supplier."
  }
];

// =============================================================================
// 2. OPERATIONS CATALOG (11 INTERMEDIATE + 14 TERMINAL)
// =============================================================================

const OPERATIONS_CATALOG = {
  // Intermediate Operations (11)
  filter: {
    name: "filter(Predicate)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: false,
    why: "Keeps only the elements that satisfy a condition (Predicate test returning true).",
    syntax: "stream.filter(n -> n > 20)",
    sampleInput: [10, 20, 30, 40, 50],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        const passed = typeof item === "number" ? item > 20 : item.length > 4;
        if (passed) {
          trace.push({ item, status: "passed", note: "passed condition (kept)" });
          output.push(item);
        } else {
          trace.push({ item, status: "removed", note: "failed condition (removed)" });
        }
      });
      return { output, trace };
    }
  },
  map: {
    name: "map(Function)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: false,
    why: "Transforms each element into another value using a Function (1-to-1 conversion).",
    syntax: "stream.map(n -> n * 2)",
    sampleInput: [10, 20, 30, 40, 50],
    returnType: "Stream<R>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        const transformed = typeof item === "number" ? item * 2 : item.toUpperCase();
        trace.push({ item, status: "transformed", note: `transformed into ${transformed}` });
        output.push(transformed);
      });
      return { output, trace };
    }
  },
  flatMap: {
    name: "flatMap(Function)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: false,
    why: "Flattens nested streams/collections into a single continuous stream (1-to-many flattening).",
    syntax: "stream.flatMap(list -> list.stream())",
    sampleInput: ["A,B", "C,D", "E"],
    returnType: "Stream<R>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        if (typeof item === "string" && item.includes(",")) {
          const parts = item.split(",");
          parts.forEach(p => output.push(p.trim()));
          trace.push({ item, status: "transformed", note: `flattened into [${parts.join(", ")}]` });
        } else {
          output.push(item);
          trace.push({ item, status: "passed", note: "flattened element" });
        }
      });
      return { output, trace };
    }
  },
  distinct: {
    name: "distinct()",
    category: "intermediate",
    lazy: true,
    stateful: true,
    shortCircuit: false,
    why: "Eliminates duplicate elements based on their equals() and hashCode() contracts.",
    syntax: "stream.distinct()",
    sampleInput: [15, 20, 15, 40, 20, 30],
    returnType: "Stream<T>",
    transform: (items) => {
      const seen = new Set();
      const trace = [];
      const output = [];
      items.forEach(item => {
        if (seen.has(item)) {
          trace.push({ item, status: "removed", note: "duplicate removed" });
        } else {
          seen.add(item);
          trace.push({ item, status: "passed", note: "first occurrence kept" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },
  sorted: {
    name: "sorted()",
    category: "intermediate",
    lazy: true,
    stateful: true,
    shortCircuit: false,
    why: "Arranges elements in natural ascending order (requires elements to implement Comparable).",
    syntax: "stream.sorted()",
    sampleInput: [45, 12, 85, 32, 19],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "transformed", note: "queued for sorting" }));
      const output = [...items].sort((a, b) => (typeof a === "number" ? a - b : String(a).localeCompare(String(b))));
      return { output, trace };
    }
  },
  sortedDesc: {
    name: "sorted(Comparator)",
    category: "intermediate",
    lazy: true,
    stateful: true,
    shortCircuit: false,
    why: "Arranges elements according to a custom Comparator (e.g. reverseOrder()).",
    syntax: "stream.sorted(Comparator.reverseOrder())",
    sampleInput: [10, 40, 20, 50, 30],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "transformed", note: "custom comparator applied" }));
      const output = [...items].sort((a, b) => (typeof a === "number" ? b - a : String(b).localeCompare(String(a))));
      return { output, trace };
    }
  },
  limit: {
    name: "limit(maxSize)",
    category: "intermediate",
    lazy: true,
    stateful: true,
    shortCircuit: true,
    why: "Truncates the stream so that it emits no more than maxSize elements.",
    syntax: "stream.limit(2)",
    sampleInput: [10, 20, 30, 40, 50],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach((item, idx) => {
        if (idx < 2) {
          trace.push({ item, status: "passed", note: `index ${idx} < 2 (kept)` });
          output.push(item);
        } else {
          trace.push({ item, status: "limit-cut", note: "limit exceeded (rejected)" });
        }
      });
      return { output, trace };
    }
  },
  skip: {
    name: "skip(n)",
    category: "intermediate",
    lazy: true,
    stateful: true,
    shortCircuit: false,
    why: "Discards the first n elements of the stream and emits the remaining elements.",
    syntax: "stream.skip(2)",
    sampleInput: [10, 20, 30, 40, 50],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach((item, idx) => {
        if (idx < 2) {
          trace.push({ item, status: "removed", note: `skipped index ${idx}` });
        } else {
          trace.push({ item, status: "passed", note: "retained" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },
  peek: {
    name: "peek(Consumer)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: false,
    why: "Performs an action on each element without altering the stream. Intended for debugging.",
    syntax: "stream.peek(System.out::println)",
    sampleInput: [10, 20, 30],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "passed", note: `peeked: ${item}` }));
      return { output: [...items], trace };
    }
  },
  takeWhile: {
    name: "takeWhile(Predicate)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: true,
    why: "Takes elements as long as predicate is true; stops at the first element that fails (Java 9+).",
    syntax: "stream.takeWhile(n -> n < 35)",
    sampleInput: [10, 20, 30, 50, 15],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      let taking = true;
      items.forEach(item => {
        if (taking && item < 35) {
          trace.push({ item, status: "passed", note: "condition met, kept" });
          output.push(item);
        } else {
          taking = false;
          trace.push({ item, status: "removed", note: "stopped taking" });
        }
      });
      return { output, trace };
    }
  },
  dropWhile: {
    name: "dropWhile(Predicate)",
    category: "intermediate",
    lazy: true,
    stateful: false,
    shortCircuit: false,
    why: "Drops elements as long as predicate is true; once an element fails, keeps all remaining (Java 9+).",
    syntax: "stream.dropWhile(n -> n < 30)",
    sampleInput: [10, 20, 35, 40, 15],
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      let dropping = true;
      items.forEach(item => {
        if (dropping && item < 30) {
          trace.push({ item, status: "removed", note: "condition met, dropped" });
        } else {
          dropping = false;
          trace.push({ item, status: "passed", note: "dropping stopped, kept" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },

  // Terminal Operations (14)
  toList: {
    name: "toList()",
    category: "terminal",
    shortCircuit: false,
    why: "Collects stream elements into an unmodifiable List (Java 16+ concise shortcut).",
    syntax: "List<Integer> list = stream.toList();",
    sampleInput: [30, 40, 50],
    returnType: "List<T>",
    execute: (items) => ({ result: `[${items.join(", ")}]`, type: `List<${typeof items[0] === "number" ? "Integer" : "String"}>` })
  },
  collect: {
    name: "collect(Collector)",
    category: "terminal",
    shortCircuit: false,
    why: "Performs a mutable reduction into a target container (e.g. toSet, joining, groupingBy).",
    syntax: "Set<Integer> set = stream.collect(Collectors.toSet());",
    sampleInput: [30, 40, 50],
    returnType: "R (Container)",
    execute: (items) => ({ result: `Set of [${[...new Set(items)].join(", ")}]`, type: "Set<T>" })
  },
  count: {
    name: "count()",
    category: "terminal",
    shortCircuit: false,
    why: "Returns the total count of elements in the stream as a 64-bit long integer.",
    syntax: "long count = stream.count();",
    sampleInput: [30, 40, 50],
    returnType: "long",
    execute: (items) => ({ result: `${items.length}L`, type: "long" })
  },
  forEach: {
    name: "forEach(Consumer)",
    category: "terminal",
    shortCircuit: false,
    why: "Performs a side-effect action for each element. Returns void.",
    syntax: "stream.forEach(System.out::println);",
    sampleInput: [10, 20, 30],
    returnType: "void",
    execute: (items) => ({ result: `Printed ${items.length} elements`, type: "void" })
  },
  forEachOrdered: {
    name: "forEachOrdered(Consumer)",
    category: "terminal",
    shortCircuit: false,
    why: "Performs an action on each element in strict encounter order (crucial in parallel streams).",
    syntax: "stream.parallel().forEachOrdered(System.out::println);",
    sampleInput: [10, 20, 30],
    returnType: "void",
    execute: (items) => ({ result: `Encounter-ordered execution complete`, type: "void" })
  },
  toArray: {
    name: "toArray()",
    category: "terminal",
    shortCircuit: false,
    why: "Returns an array containing all of the elements in this stream.",
    syntax: "Integer[] arr = stream.toArray(Integer[]::new);",
    sampleInput: [10, 20, 30],
    returnType: "T[]",
    execute: (items) => ({ result: `{${items.join(", ")}}`, type: "T[]" })
  },
  min: {
    name: "min(Comparator)",
    category: "terminal",
    shortCircuit: false,
    why: "Returns the minimum element according to the comparator, wrapped in an Optional.",
    syntax: "Optional<Integer> min = stream.min(Integer::compare);",
    sampleInput: [30, 10, 50],
    returnType: "Optional<T>",
    execute: (items) => {
      if (!items.length) return { result: "Optional.empty", type: "Optional<T>" };
      const val = [...items].sort((a, b) => a - b)[0];
      return { result: `Optional[${val}]`, type: "Optional<T>" };
    }
  },
  max: {
    name: "max(Comparator)",
    category: "terminal",
    shortCircuit: false,
    why: "Returns the maximum element according to the comparator, wrapped in an Optional.",
    syntax: "Optional<Integer> max = stream.max(Integer::compare);",
    sampleInput: [30, 10, 50],
    returnType: "Optional<T>",
    execute: (items) => {
      if (!items.length) return { result: "Optional.empty", type: "Optional<T>" };
      const val = [...items].sort((a, b) => b - a)[0];
      return { result: `Optional[${val}]`, type: "Optional<T>" };
    }
  },
  findFirst: {
    name: "findFirst()",
    category: "terminal",
    shortCircuit: true,
    why: "Returns an Optional describing the first element; short-circuits immediately.",
    syntax: "Optional<Integer> first = stream.findFirst();",
    sampleInput: [10, 20, 30],
    returnType: "Optional<T>",
    execute: (items) => ({ result: items.length ? `Optional[${items[0]}]` : "Optional.empty", type: "Optional<T>" })
  },
  findAny: {
    name: "findAny()",
    category: "terminal",
    shortCircuit: true,
    why: "Returns an Optional describing some element; optimized for parallel performance.",
    syntax: "Optional<Integer> any = stream.findAny();",
    sampleInput: [10, 20, 30],
    returnType: "Optional<T>",
    execute: (items) => ({ result: items.length ? `Optional[${items[0]}]` : "Optional.empty", type: "Optional<T>" })
  },
  anyMatch: {
    name: "anyMatch(Predicate)",
    category: "terminal",
    shortCircuit: true,
    why: "Returns true if AT LEAST ONE element matches. Short-circuits on first true.",
    syntax: "boolean any = stream.anyMatch(n -> n > 20);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    execute: (items) => {
      const match = items.some(n => (typeof n === "number" ? n > 20 : n.length > 4));
      return { result: `${match}`, type: "boolean" };
    }
  },
  allMatch: {
    name: "allMatch(Predicate)",
    category: "terminal",
    shortCircuit: true,
    why: "Returns true if EVERY element matches. Short-circuits on first false.",
    syntax: "boolean all = stream.allMatch(n -> n > 0);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    execute: (items) => {
      const match = items.every(n => (typeof n === "number" ? n > 0 : n.length > 0));
      return { result: `${match}`, type: "boolean" };
    }
  },
  noneMatch: {
    name: "noneMatch(Predicate)",
    category: "terminal",
    shortCircuit: true,
    why: "Returns true if ZERO elements match. Short-circuits on first match.",
    syntax: "boolean none = stream.noneMatch(n -> n < 0);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    execute: (items) => {
      const match = items.every(n => (typeof n === "number" ? n >= 0 : true));
      return { result: `${match}`, type: "boolean" };
    }
  },
  reduce: {
    name: "reduce(identity, accumulator)",
    category: "terminal",
    shortCircuit: false,
    why: "Folds all elements into a single aggregate value using an associative binary operator.",
    syntax: "int sum = stream.reduce(0, (a, b) -> a + b);",
    sampleInput: [10, 20, 30],
    returnType: "T / Optional<T>",
    execute: (items) => {
      const sum = items.reduce((acc, curr) => (typeof curr === "number" ? acc + curr : acc + " " + curr), 0);
      return { result: `${sum}`, type: typeof items[0] === "number" ? "Integer" : "String" };
    }
  }
};

// =============================================================================
// 3. INTERVIEW QUESTIONS (7 QUESTIONS)
// =============================================================================

const INTERVIEW_QUESTIONS = [
  {
    category: "Return Types",
    question: "What does filter() return?",
    options: [
      "Stream<T> (Another Stream)",
      "List<T> containing the matching elements",
      "boolean indicating whether any element matched",
      "void"
    ],
    correctIndex: 0,
    why: "filter() is an intermediate operation. Intermediate operations in Java ALWAYS return another Stream<T>, allowing multiple operations to be chained together lazily.",
    code: "Stream<Integer> stream = numbers.stream().filter(n -> n > 20);\n// filter() does NOT return a List; it returns a Stream!"
  },
  {
    category: "Transformations",
    question: "What is the key difference between map() and flatMap()?",
    options: [
      "map() is 1-to-1, while flatMap() is 1-to-many flattening nested streams into one stream",
      "map() is lazy, while flatMap() is eager",
      "map() works only on numbers, flatMap() works on strings",
      "flatMap() sorts the elements automatically"
    ],
    correctIndex: 0,
    why: "map() transforms each element T into a single R (1-to-1). flatMap() transforms each element T into a Stream<R> and then flattens all those streams into a single Stream<R>.",
    code: "// map() creates a Stream of Lists: Stream<List<String>>\n// flatMap() flattens into one Stream of Strings: Stream<String>"
  },
  {
    category: "Pipeline Lifecycle",
    question: "Which operation terminates a Stream?",
    options: [
      "Terminal operations (e.g. toList, count, findFirst, reduce)",
      "filter()",
      "sorted()",
      "distinct()"
    ],
    correctIndex: 0,
    why: "Only terminal operations consume and close a Stream. Intermediate operations return a new Stream and wait lazily for a terminal operation to trigger execution.",
    code: "numbers.stream().filter(n -> n > 20); // NOT terminated!\nnumbers.stream().filter(n -> n > 20).toList(); // TERMINATED!"
  },
  {
    category: "Short-Circuiting",
    question: "What does findFirst() return?",
    options: [
      "Optional<T>",
      "T directly (throws NullPointerException if empty)",
      "boolean",
      "Stream<T>"
    ],
    correctIndex: 0,
    why: "findFirst() returns an Optional<T> to prevent NullPointerException if the stream has no matching elements.",
    code: "Optional<Integer> first = stream.filter(n -> n > 20).findFirst();\nfirst.ifPresent(System.out::println);"
  },
  {
    category: "Lazy Evaluation",
    question: "Are intermediate operations lazy in Java Streams?",
    options: [
      "Yes, they do zero work until a terminal operation is called",
      "No, they process items immediately upon declaration",
      "Only sorted() is lazy, filter() is eager",
      "They are lazy only when run in parallel"
    ],
    correctIndex: 0,
    why: "Intermediate operations only construct the pipeline recipe. Zero iterations happen until a terminal operation requests data.",
    code: "Stream<Integer> s = numbers.stream().filter(n -> { System.out.println(n); return n > 20; });\n// Nothing prints to the console until a terminal op is attached!"
  },
  {
    category: "Pitfalls",
    question: "What happens if you reuse a Stream after calling a terminal operation on it?",
    options: [
      "IllegalStateException: stream has already been operated upon or closed",
      "It re-executes the pipeline from the beginning",
      "It returns an empty list",
      "NullPointerException"
    ],
    correctIndex: 0,
    why: "Streams are single-use consumable pipelines. Once a terminal operation is invoked, the stream is closed and cannot be traversed again.",
    code: "Stream<String> s = list.stream();\ns.forEach(System.out::println);\ns.count(); // 💥 Throws IllegalStateException!"
  },
  {
    category: "Primitive Specialization",
    question: "What is the difference between Stream<Integer> and IntStream?",
    options: [
      "IntStream operates directly on primitive ints without autoboxing overhead",
      "Stream<Integer> is faster than IntStream",
      "IntStream does not support filter() or map()",
      "There is no difference"
    ],
    correctIndex: 0,
    why: "Stream<Integer> boxes every primitive into an Integer heap object. IntStream avoids heap allocations and provides numerical primitives like sum(), average(), and summaryStatistics().",
    code: "IntStream intStream = Arrays.stream(new int[]{1, 2, 3});\nint sum = intStream.sum(); // Direct primitive addition!"
  }
];

// =============================================================================
// 4. TOP 6 STREAM PITFALLS
// =============================================================================

const PITFALLS_CATALOG = [
  {
    title: "1. Reusing a Stream",
    tag: "danger",
    wrongCode: "Stream<String> s = list.stream();\ns.forEach(System.out::println);\nlong count = s.count(); // 💥 IllegalStateException!",
    correctCode: "list.stream().forEach(System.out::println);\nlong count = list.stream().count(); // ✅ Create new stream",
    why: "A Java Stream is single-use. Once a terminal operation is called, the pipeline is closed forever."
  },
  {
    title: "2. Wrong Operation Order",
    tag: "warning",
    wrongCode: "// Slow: transforms 100,000 items, then filters\nstream.map(expensiveFn).filter(pred);",
    correctCode: "// Fast: filters first, transforms only 5 matching items\nstream.filter(pred).map(expensiveFn);",
    why: "Placing filter() before map() or sorted() prevents unnecessary computations on elements that will be discarded."
  },
  {
    title: "3. Infinite Stream without limit()",
    tag: "danger",
    wrongCode: "// 💥 Infinite loop! Memory OutOfMemoryError!\nStream.iterate(0, n -> n + 1)\n  .collect(Collectors.toList());",
    correctCode: "Stream.iterate(0, n -> n + 1)\n  .limit(10) // ✅ Bound stream size\n  .toList();",
    why: "Infinite generators like iterate() and generate() must be bounded with limit() or takeWhile() before terminal collection."
  },
  {
    title: "4. Side Effects inside forEach()",
    tag: "warning",
    wrongCode: "List<Integer> result = new ArrayList<>();\nstream.parallel().forEach(result::add); // 💥 Race condition!",
    correctCode: "// ✅ Safe, thread-safe, and idiomatic\nList<Integer> result = stream.toList();",
    why: "Mutating shared collections inside forEach breaks thread-safety. Use toList() or collect() instead."
  },
  {
    title: "5. Confusing map() and flatMap()",
    tag: "info",
    wrongCode: "// Results in Stream<List<String>> instead of Stream<String>\norders.stream().map(order -> order.getItems());",
    correctCode: "// Correctly flattens into Stream<String>\norders.stream().flatMap(order -> order.getItems().stream());",
    why: "Use map() when each element transforms into 1 value. Use flatMap() when each element transforms into a stream or list that needs flattening."
  },
  {
    title: "6. Thinking Intermediate Ops Execute Immediately",
    tag: "info",
    wrongCode: "Stream<Integer> s = list.stream()\n  .filter(n -> { System.out.println(n); return n > 10; });\n// Nothing prints!",
    correctCode: "// ✅ Terminal operation pulls data and triggers execution\ns.toList();",
    why: "Intermediate operations are 100% lazy declarations. Zero iterations happen until a terminal operation is attached."
  }
];

// =============================================================================
// 5. APPLICATION STATE
// =============================================================================

const state = {
  activeTab: "builder",
  theme: "dark",

  // Builder State
  builder: {
    mode: "all", // "all" or "one" (Process All vs Process One Element)
    sourceKey: "numbers-default",
    sourceData: [10, 20, 30, 40, 50],
    sourceType: "List<Integer>",
    sourceSyntax: "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);",
    intermediateOps: [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" },
      { id: "map-2", opKey: "map", param: "n -> n * 2", name: "map(n * 2)" },
      { id: "sorted-3", opKey: "sorted", param: "", name: "sorted()" }
    ],
    terminalOp: "toList",
    currentStepIndex: 0,
    isRunning: false,
    stepHistory: [],
    
    // Single element mode tracking
    singleElIndex: 0,
    singleStageIndex: 0
  },

  // Operations Explorer Selected Op
  selectedOpKey: "filter",

  // Lazy Lab State
  lazy: {
    elements: [10, 20, 30, 40, 50],
    currentIndex: -1,
    hasTerminal: true,
    isComplete: false,
    inspectedCount: 0,
    filterCount: 0,
    mapCount: 0
  },

  // Quiz State
  quiz: {
    currentIndex: 0,
    userAnswers: {}
  }
};

// =============================================================================
// 6. INITIALIZATION & DOM BINDINGS
// =============================================================================

document.addEventListener("DOMContentLoaded", () => {
  initTheme();
  initNavigation();
  initHeroNodes();
  initSourcesTab();
  initOpsExplorerTab();
  initBuilderTab();
  initLazyLabTab();
  initComparisonTab();
  initInterviewTab();
  initPitfallsTab();
});

// Theme Management
function initTheme() {
  const toggleBtn = document.getElementById("themeToggleBtn");
  const savedTheme = localStorage.getItem("java-stream-theme") || "dark";
  setTheme(savedTheme);

  toggleBtn.addEventListener("click", () => {
    const nextTheme = state.theme === "dark" ? "light" : "dark";
    setTheme(nextTheme);
  });
}

function setTheme(theme) {
  state.theme = theme;
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem("java-stream-theme", theme);
}

// Navigation Handling
function initNavigation() {
  const tabs = document.querySelectorAll(".nav-tab");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      switchTab(tab.dataset.tab);
    });
  });
}

function switchTab(tabId) {
  document.querySelectorAll(".nav-tab").forEach(t => {
    t.classList.toggle("active", t.dataset.tab === tabId);
  });
  document.querySelectorAll(".tab-pane").forEach(pane => {
    pane.classList.toggle("active", pane.id === `tab-${tabId}`);
  });
  state.activeTab = tabId;
  window.scrollTo({ top: 0, behavior: "smooth" });
}

// Hero Nodes Interaction
function initHeroNodes() {
  document.getElementById("heroNodeSource").addEventListener("click", () => switchTab("sources"));
  document.getElementById("heroNodeInter").addEventListener("click", () => {
    switchTab("operations");
    const interPill = document.querySelector('.filter-pill[data-filter="intermediate"]');
    if (interPill) interPill.click();
  });
  document.getElementById("heroNodeTerm").addEventListener("click", () => {
    switchTab("operations");
    const termPill = document.querySelector('.filter-pill[data-filter="terminal"]');
    if (termPill) termPill.click();
  });
}

// =============================================================================
// 7. TAB 1: BUILD YOUR STREAM (PIPELINE BUILDER)
// =============================================================================

function initBuilderTab() {
  const sourceSelect = document.getElementById("builderSourceSelect");
  const customWrap = document.getElementById("customSourceInputWrap");
  const customInput = document.getElementById("customSourceInput");
  const applyCustomBtn = document.getElementById("applyCustomSourceBtn");
  const addOpDropdownBtn = document.getElementById("addOpDropdownBtn");
  const addOpMenu = document.getElementById("addOpMenu");
  const terminalSelect = document.getElementById("builderTerminalSelect");
  const runBtn = document.getElementById("runPipelineBtn");
  const prevBtn = document.getElementById("prevStepBtn");
  const nextBtn = document.getElementById("nextStepBtn");
  const resetBtn = document.getElementById("resetPipelineBtn");
  const copyCodeBtn = document.getElementById("copyCodeBtn");

  // Mode segmented control
  const modeAllBtn = document.getElementById("modeProcessAllBtn");
  const modeOneBtn = document.getElementById("modeProcessOneBtn");

  modeAllBtn.addEventListener("click", () => {
    state.builder.mode = "all";
    modeAllBtn.classList.add("active");
    modeOneBtn.classList.remove("active");
    document.getElementById("singleElementHud").classList.add("hidden");
    resetPipeline();
  });

  modeOneBtn.addEventListener("click", () => {
    state.builder.mode = "one";
    modeOneBtn.classList.add("active");
    modeAllBtn.classList.remove("active");
    document.getElementById("singleElementHud").classList.remove("hidden");
    state.builder.singleElIndex = 0;
    state.builder.singleStageIndex = 0;
    updateSingleElementHud();
    resetPipeline();
  });

  // Source selection change
  sourceSelect.addEventListener("change", (e) => {
    const val = e.target.value;
    if (val === "custom") {
      customWrap.classList.remove("hidden");
    } else {
      customWrap.classList.add("hidden");
      applySourcePreset(val);
    }
  });

  applyCustomBtn.addEventListener("click", () => {
    const raw = customInput.value.trim();
    if (!raw) return;
    const parsed = raw.split(",").map(s => {
      const num = Number(s.trim());
      return isNaN(num) ? s.trim() : num;
    });
    state.builder.sourceData = parsed;
    state.builder.sourceType = typeof parsed[0] === "number" ? "List<Integer>" : "List<String>";
    state.builder.sourceSyntax = `List<${typeof parsed[0] === "number" ? "Integer" : "String"}> customList = Arrays.asList(${parsed.map(x => typeof x === "string" ? `"${x}"` : x).join(", ")});`;
    renderBuilderSourceTokens();
    rebuildPipelineTrack();
    updateTypeProgression();
    updateGeneratedCode();
  });

  // Add Operation Dropdown
  addOpDropdownBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    addOpMenu.classList.toggle("show");
  });

  document.addEventListener("click", () => {
    addOpMenu.classList.remove("show");
  });

  addOpMenu.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      addIntermediateOperation(btn.dataset.addOp);
      addOpMenu.classList.remove("show");
    });
  });

  // Terminal selector change
  terminalSelect.addEventListener("change", (e) => {
    state.builder.terminalOp = e.target.value;
    updateTerminalSummary();
    rebuildPipelineTrack();
    updateTypeProgression();
    updateGeneratedCode();
  });

  // Presets Bar Handlers (8 Presets)
  initPresetsBar();

  // Playback Control Buttons
  runBtn.addEventListener("click", () => {
    if (state.builder.mode === "one") {
      stepSingleElement(1);
    } else {
      runAnimatedPipeline();
    }
  });

  nextBtn.addEventListener("click", () => {
    if (state.builder.mode === "one") {
      stepSingleElement(1);
    } else {
      stepPipeline(1);
    }
  });

  prevBtn.addEventListener("click", () => {
    if (state.builder.mode === "one") {
      stepSingleElement(-1);
    } else {
      stepPipeline(-1);
    }
  });

  resetBtn.addEventListener("click", () => {
    resetPipeline();
  });

  // Copy Code button
  copyCodeBtn.addEventListener("click", () => {
    const code = document.getElementById("generatedJavaCode").innerText;
    navigator.clipboard.writeText(code).then(() => {
      copyCodeBtn.innerHTML = `✓ Copied!`;
      setTimeout(() => {
        copyCodeBtn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg> Copy Code`;
      }, 1500);
    });
  });

  // Initial Render
  renderBuilderSourceTokens();
  renderBuilderOpsList();
  updateTerminalSummary();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function initPresetsBar() {
  const presetBtns = document.querySelectorAll(".preset-btn");
  presetBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      presetBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      loadPreset(btn.dataset.preset);
    });
  });
}

function loadPreset(presetKey) {
  if (presetKey === "filter-even") {
    state.builder.sourceData = [10, 15, 20, 25, 30, 35, 40];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 15, 20, 25, 30, 35, 40);";
    state.builder.intermediateOps = [
      { id: "filter-even", opKey: "filter", param: "n -> n % 2 == 0", name: "filter(n -> n % 2 == 0)" }
    ];
    state.builder.terminalOp = "toList";
  } else if (presetKey === "filter-map-sort") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" },
      { id: "map-2", opKey: "map", param: "n -> n * 2", name: "map(n * 2)" },
      { id: "sorted-3", opKey: "sorted", param: "", name: "sorted()" }
    ];
    state.builder.terminalOp = "toList";
  } else if (presetKey === "remove-duplicates") {
    state.builder.sourceData = [15, 20, 15, 40, 20, 30];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(15, 20, 15, 40, 20, 30);";
    state.builder.intermediateOps = [
      { id: "distinct-1", opKey: "distinct", param: "", name: "distinct()" },
      { id: "sorted-2", opKey: "sorted", param: "", name: "sorted()" }
    ];
    state.builder.terminalOp = "toList";
  } else if (presetKey === "find-first-match") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" }
    ];
    state.builder.terminalOp = "findFirst";
  } else if (presetKey === "calc-sum-reduce") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" }
    ];
    state.builder.terminalOp = "reduce";
  } else if (presetKey === "find-maximum") {
    state.builder.sourceData = [45, 12, 85, 32, 19, 64];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(45, 12, 85, 32, 19, 64);";
    state.builder.intermediateOps = [];
    state.builder.terminalOp = "max";
  } else if (presetKey === "count-matching") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" }
    ];
    state.builder.terminalOp = "count";
  } else if (presetKey === "transform-strings") {
    state.builder.sourceData = ["apple", "banana", "kiwi", "pear", "orange"];
    state.builder.sourceType = "List<String>";
    state.builder.sourceSyntax = 'List<String> fruits = Arrays.asList("apple", "banana", "kiwi", "pear", "orange");';
    state.builder.intermediateOps = [
      { id: "map-str", opKey: "map", param: "String::toUpperCase", name: "map(String::toUpperCase)" },
      { id: "sorted-str", opKey: "sorted", param: "", name: "sorted()" }
    ];
    state.builder.terminalOp = "toList";
  }

  document.getElementById("builderTerminalSelect").value = state.builder.terminalOp;
  renderBuilderSourceTokens();
  renderBuilderOpsList();
  updateTerminalSummary();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function applySourcePreset(key) {
  if (key === "numbers-default") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
  } else if (key === "numbers-duplicates") {
    state.builder.sourceData = [15, 20, 15, 40, 20, 30];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(15, 20, 15, 40, 20, 30);";
  } else if (key === "numbers-unsorted") {
    state.builder.sourceData = [45, 12, 85, 32, 19, 64];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(45, 12, 85, 32, 19, 64);";
  } else if (key === "strings-fruits") {
    state.builder.sourceData = ["apple", "banana", "kiwi", "pear", "orange"];
    state.builder.sourceType = "List<String>";
    state.builder.sourceSyntax = 'List<String> fruits = Arrays.asList("apple", "banana", "kiwi", "pear", "orange");';
  } else if (key === "primitive-ints") {
    state.builder.sourceData = [5, 12, 18, 24, 30];
    state.builder.sourceType = "int[]";
    state.builder.sourceSyntax = "int[] primitives = {5, 12, 18, 24, 30};";
  }
  renderBuilderSourceTokens();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function renderBuilderSourceTokens() {
  const container = document.getElementById("builderSourceTokens");
  container.innerHTML = "";
  state.builder.sourceData.forEach(item => {
    const token = document.createElement("div");
    token.className = "token";
    token.textContent = item;
    container.appendChild(token);
  });
  document.getElementById("builderSourceSyntax").textContent = state.builder.sourceSyntax;
}

function renderBuilderOpsList() {
  const container = document.getElementById("builderOpsList");
  container.innerHTML = "";

  if (state.builder.intermediateOps.length === 0) {
    container.innerHTML = `<div class="empty-ops-hint">No intermediate operations added. Stream will flow directly from source to terminal operation.</div>`;
    return;
  }

  state.builder.intermediateOps.forEach((op, index) => {
    const item = document.createElement("div");
    item.className = "op-chain-item";
    item.innerHTML = `
      <div class="op-chain-item-info">
        <div class="op-chain-name">${op.name}</div>
        <div class="op-chain-type-meta">Intermediate &bull; Returns Stream&lt;T&gt;</div>
      </div>
      <div class="op-chain-controls">
        <button class="btn-ctrl btn-ctrl-up" title="Move Up" data-up-idx="${index}" ${index === 0 ? "disabled" : ""}>▲</button>
        <button class="btn-ctrl btn-ctrl-down" title="Move Down" data-down-idx="${index}" ${index === state.builder.intermediateOps.length - 1 ? "disabled" : ""}>▼</button>
        <button class="btn-ctrl btn-ctrl-remove" title="Remove" data-remove-idx="${index}">✕</button>
      </div>
    `;

    // Up button
    item.querySelector("[data-up-idx]").addEventListener("click", () => {
      if (index > 0) {
        const temp = state.builder.intermediateOps[index];
        state.builder.intermediateOps[index] = state.builder.intermediateOps[index - 1];
        state.builder.intermediateOps[index - 1] = temp;
        refreshPipeline();
      }
    });

    // Down button
    item.querySelector("[data-down-idx]").addEventListener("click", () => {
      if (index < state.builder.intermediateOps.length - 1) {
        const temp = state.builder.intermediateOps[index];
        state.builder.intermediateOps[index] = state.builder.intermediateOps[index + 1];
        state.builder.intermediateOps[index + 1] = temp;
        refreshPipeline();
      }
    });

    // Remove button
    item.querySelector("[data-remove-idx]").addEventListener("click", () => {
      state.builder.intermediateOps.splice(index, 1);
      refreshPipeline();
    });

    container.appendChild(item);
  });
}

function refreshPipeline() {
  renderBuilderOpsList();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function addIntermediateOperation(opKey) {
  let name = `${opKey}()`;
  let param = "";

  if (opKey === "filter") {
    param = "n -> n > 20";
    name = "filter(n -> n > 20)";
  } else if (opKey === "map") {
    param = "n -> n * 2";
    name = "map(n -> n * 2)";
  } else if (opKey === "limit") {
    param = "2";
    name = "limit(2)";
  } else if (opKey === "skip") {
    param = "2";
    name = "skip(2)";
  } else if (opKey === "takeWhile") {
    param = "n -> n < 35";
    name = "takeWhile(n -> n < 35)";
  } else if (opKey === "dropWhile") {
    param = "n -> n < 30";
    name = "dropWhile(n -> n < 30)";
  } else if (opKey === "peek") {
    param = "System.out::println";
    name = "peek(System.out::println)";
  } else if (opKey === "sortedDesc") {
    param = "Comparator.reverseOrder()";
    name = "sorted(Comparator.reverseOrder())";
  }

  state.builder.intermediateOps.push({
    id: `${opKey}-${Date.now()}`,
    opKey,
    param,
    name
  });

  refreshPipeline();
}

function updateTerminalSummary() {
  const termKey = state.builder.terminalOp;
  const termDef = OPERATIONS_CATALOG[termKey] || OPERATIONS_CATALOG.toList;
  document.getElementById("termShortCircuitLabel").textContent = termDef.shortCircuit ? "YES (Short-circuits!)" : "NO";
  document.getElementById("termReturnLabel").textContent = termDef.returnType;
}

/**
 * Rebuilds the visual Pipeline Track with Real Data Tokens
 */
function rebuildPipelineTrack() {
  const track = document.getElementById("pipelineTrack");
  track.innerHTML = "";

  // 1. Source Node
  const sourceNode = document.createElement("div");
  sourceNode.className = "stage-node source-stage";
  sourceNode.id = "stage-source";
  sourceNode.innerHTML = `
    <div class="stage-header">
      <span class="stage-type-tag source">1. Stream Source</span>
      <div class="stage-title">${state.builder.sourceType}</div>
      <span class="stage-subtitle">Original Data Container</span>
    </div>
    <div class="stage-tokens-flow" id="tokens-flow-source">
      ${state.builder.sourceData.map(d => `<div class="token">${d}</div>`).join("")}
    </div>
  `;
  track.appendChild(sourceNode);

  // Arrow
  track.appendChild(createConnectorArrow());

  // 2. Stream() Init Node
  const streamNode = document.createElement("div");
  streamNode.className = "stage-node intermediate-stage";
  streamNode.id = "stage-stream-init";
  streamNode.innerHTML = `
    <div class="stage-header">
      <span class="stage-type-tag intermediate">🟢 Intermediate Init</span>
      <div class="stage-title">.stream()</div>
      <span class="stage-subtitle">Wraps data into Stream&lt;T&gt;</span>
    </div>
    <div class="stage-tokens-flow" id="tokens-flow-stream-init">
      ${state.builder.sourceData.map(d => `<div class="token">${d}</div>`).join("")}
    </div>
  `;
  track.appendChild(streamNode);

  // 3. Intermediate Operations Nodes
  let currentItems = [...state.builder.sourceData];
  state.builder.stepHistory = [
    { stage: "source", items: [...currentItems], note: "Original data loaded into memory." },
    { stage: "stream-init", items: [...currentItems], note: ".stream() created continuous pipeline wrapper." }
  ];

  state.builder.intermediateOps.forEach((op, idx) => {
    track.appendChild(createConnectorArrow());

    const opDef = OPERATIONS_CATALOG[op.opKey];
    const { output, trace } = opDef ? opDef.transform(currentItems) : { output: currentItems, trace: [] };

    // Build token elements with visual statuses:
    // e.g. [10] ❌, [20] ❌, [30] ✅
    let tokensHtml = "";
    if (op.opKey === "filter") {
      tokensHtml = trace.map(t => {
        if (t.status === "passed") {
          return `<div class="token token-passed" title="${t.note}">✅ ${t.item}</div>`;
        } else {
          return `<div class="token token-removed" title="${t.note}">❌ ${t.item}</div>`;
        }
      }).join("") + ` <span style="color:var(--text-muted); font-size:0.8rem;">→</span> ` +
      output.map(o => `<div class="token token-passed">${o}</div>`).join("");
    } else if (op.opKey === "map") {
      tokensHtml = trace.map(t => `<div class="token token-transformed" title="${t.note}">⚡ ${t.item}</div>`).join("") +
        ` <span style="color:var(--text-muted); font-size:0.8rem;">→</span> ` +
        output.map(o => `<div class="token token-transformed">${o}</div>`).join("");
    } else if (op.opKey === "limit") {
      tokensHtml = trace.map(t => {
        if (t.status === "passed") {
          return `<div class="token token-passed">${t.item}</div>`;
        } else {
          return `<div class="token token-limit-cut" title="Beyond limit">${t.item} (Cut)</div>`;
        }
      }).join("");
    } else if (op.opKey === "distinct") {
      tokensHtml = trace.map(t => {
        if (t.status === "passed") {
          return `<div class="token token-passed">${t.item}</div>`;
        } else {
          return `<div class="token token-removed" title="Duplicate">${t.item} (Dup)</div>`;
        }
      }).join("");
    } else {
      tokensHtml = output.map(d => `<div class="token">${d}</div>`).join("");
    }

    const opNode = document.createElement("div");
    opNode.className = "stage-node intermediate-stage";
    opNode.id = `stage-op-${idx}`;
    opNode.innerHTML = `
      <div class="stage-header">
        <span class="stage-type-tag intermediate">🟢 Intermediate #${idx + 1}</span>
        <div class="stage-title">${op.name}</div>
        <span class="stage-subtitle">Returns Stream&lt;T&gt; &bull; Continues</span>
      </div>
      <div class="stage-tokens-flow" id="tokens-flow-op-${idx}">
        ${tokensHtml}
      </div>
    `;
    track.appendChild(opNode);

    state.builder.stepHistory.push({
      stage: `op-${idx}`,
      opName: op.name,
      items: [...output],
      trace,
      note: `${op.name} processed data: ${output.length} elements remain in stream.`
    });

    currentItems = output;
  });

  // 4. Terminal Operation Node
  track.appendChild(createConnectorArrow());

  const termKey = state.builder.terminalOp;
  const termDef = OPERATIONS_CATALOG[termKey] || OPERATIONS_CATALOG.toList;
  const terminalExec = termDef.execute ? termDef.execute(currentItems) : { result: "Done", type: "void" };

  const termNode = document.createElement("div");
  termNode.className = "stage-node terminal-stage";
  termNode.id = "stage-terminal";
  termNode.innerHTML = `
    <div class="stage-header">
      <span class="stage-type-tag terminal">🟠 Terminal Operation</span>
      <div class="stage-title">.${termKey}()</div>
      <span class="stage-subtitle">Consumes Stream &bull; Closes Pipeline</span>
    </div>
    <div class="stage-tokens-flow" id="tokens-flow-terminal">
      <div class="token token-transformed" style="font-size: 0.95rem;">${terminalExec.result}</div>
    </div>
  `;
  track.appendChild(termNode);

  // 5. Final Result Node
  track.appendChild(createConnectorArrow());

  const resultNode = document.createElement("div");
  resultNode.className = "stage-node result-stage";
  resultNode.id = "stage-result";
  resultNode.innerHTML = `
    <div class="stage-header">
      <span class="stage-type-tag result">Final Result</span>
      <div class="stage-title">${terminalExec.type}</div>
      <span class="stage-subtitle">Pipeline Terminated & Data Produced</span>
    </div>
    <div class="stage-tokens-flow" id="tokens-flow-result">
      <span style="font-family: var(--font-mono); font-weight: 800; color: #58a6ff; font-size: 1.15rem;">
        ${terminalExec.result}
      </span>
    </div>
  `;
  track.appendChild(resultNode);

  state.builder.stepHistory.push({
    stage: "terminal",
    items: [terminalExec.result],
    result: terminalExec.result,
    type: terminalExec.type,
    note: `Terminal operation .${termKey}() triggered execution and produced final result: ${terminalExec.result}. Stream is now closed!`
  });
}

function createConnectorArrow() {
  const arrow = document.createElement("div");
  arrow.className = "stage-connector-arrow";
  arrow.innerHTML = `↓`;
  return arrow;
}

/**
 * Return Type Progression chain
 */
function updateTypeProgression() {
  const container = document.getElementById("typeChainDisplay");
  container.innerHTML = "";

  const elements = [];
  elements.push({ label: state.builder.sourceType, type: "source" });
  elements.push({ label: "Stream<T>", type: "stream" });

  state.builder.intermediateOps.forEach(op => {
    if (op.opKey === "map" && typeof state.builder.sourceData[0] === "number") {
      elements.push({ label: "Stream<Integer>", type: "stream" });
    } else {
      elements.push({ label: "Stream<T>", type: "stream" });
    }
  });

  const termDef = OPERATIONS_CATALOG[state.builder.terminalOp] || OPERATIONS_CATALOG.toList;
  elements.push({ label: termDef.returnType, type: "terminal" });

  elements.forEach((item, idx) => {
    const pill = document.createElement("span");
    pill.className = `type-pill ${item.type} ${idx === state.builder.currentStepIndex ? "highlight-type" : ""}`;
    pill.textContent = item.label;
    container.appendChild(pill);

    if (idx < elements.length - 1) {
      const arrow = document.createElement("span");
      arrow.className = "type-arrow";
      arrow.textContent = "→";
      container.appendChild(arrow);
    }
  });
}

/**
 * Java Code Generator
 */
function updateGeneratedCode() {
  const codeBox = document.getElementById("generatedJavaCode");
  const termKey = state.builder.terminalOp;
  const termDef = OPERATIONS_CATALOG[termKey] || OPERATIONS_CATALOG.toList;

  let code = `${state.builder.sourceSyntax}\n\n`;
  code += `${termDef.returnType} result = numbers.stream()\n`;

  state.builder.intermediateOps.forEach(op => {
    code += `    .${op.name}\n`;
  });

  if (termKey === "reduce") {
    code += `    .reduce(0, (a, b) -> a + b);\n`;
  } else if (termKey === "min" || termKey === "max") {
    code += `    .${termKey}(Integer::compare);\n`;
  } else if (termKey === "anyMatch") {
    code += `    .anyMatch(n -> n > 20);\n`;
  } else if (termKey === "allMatch") {
    code += `    .allMatch(n -> n > 0);\n`;
  } else if (termKey === "noneMatch") {
    code += `    .noneMatch(n -> n < 0);\n`;
  } else {
    code += `    .${termKey}();\n`;
  }

  code += `\nSystem.out.println("Result: " + result);`;
  codeBox.textContent = code;
}

/**
 * Animated pipeline runner for "Process All"
 */
function runAnimatedPipeline() {
  const runBtnLabel = document.getElementById("runBtnLabel");
  const statusBadge = document.getElementById("pipelineStatusBadge");
  const prevBtn = document.getElementById("prevStepBtn");
  const nextBtn = document.getElementById("nextStepBtn");
  const speed = parseInt(document.getElementById("speedSelect").value, 10);

  state.builder.isRunning = true;
  state.builder.currentStepIndex = 0;
  statusBadge.textContent = "Executing...";
  statusBadge.className = "status-indicator-badge running";
  runBtnLabel.textContent = "Pause";
  prevBtn.disabled = true;
  nextBtn.disabled = true;

  highlightStage(0);

  const totalSteps = state.builder.stepHistory.length;
  let step = 0;

  const timer = setInterval(() => {
    step++;
    if (step >= totalSteps || !state.builder.isRunning) {
      clearInterval(timer);
      state.builder.isRunning = false;
      statusBadge.textContent = "Completed";
      statusBadge.className = "status-indicator-badge completed";
      runBtnLabel.textContent = "Run Pipeline";
      prevBtn.disabled = false;
      nextBtn.disabled = false;
      return;
    }

    state.builder.currentStepIndex = step;
    highlightStage(step);
    updateTypeProgression();
  }, speed);
}

function stepPipeline(delta) {
  const newIndex = state.builder.currentStepIndex + delta;
  if (newIndex >= 0 && newIndex < state.builder.stepHistory.length) {
    state.builder.currentStepIndex = newIndex;
    highlightStage(newIndex);
    updateTypeProgression();
  }
}

function highlightStage(stepIndex) {
  const allStages = document.querySelectorAll(".stage-node");
  allStages.forEach(s => s.classList.remove("active-stage"));

  const stepInfo = state.builder.stepHistory[stepIndex];
  if (!stepInfo) return;

  const targetStage = document.getElementById(`stage-${stepInfo.stage}`) || allStages[stepIndex];
  if (targetStage) {
    targetStage.classList.add("active-stage");
    targetStage.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  document.getElementById("stepNarrativeText").innerHTML = `<strong>Stage ${stepIndex + 1}/${state.builder.stepHistory.length}:</strong> ${stepInfo.note}`;
  document.getElementById("prevStepBtn").disabled = (stepIndex === 0);
  document.getElementById("nextStepBtn").disabled = (stepIndex === state.builder.stepHistory.length - 1);
}

/**
 * Single Element Stepper Engine
 */
function stepSingleElement(delta) {
  const elements = state.builder.sourceData;
  const stagesCount = state.builder.intermediateOps.length + 2; // source + ops + terminal

  let sIdx = state.builder.singleStageIndex + delta;
  let eIdx = state.builder.singleElIndex;

  if (sIdx >= stagesCount) {
    sIdx = 0;
    eIdx++;
  } else if (sIdx < 0) {
    if (eIdx > 0) {
      eIdx--;
      sIdx = stagesCount - 1;
    } else {
      sIdx = 0;
    }
  }

  if (eIdx >= elements.length) {
    document.getElementById("pipelineStatusBadge").textContent = "All Elements Evaluated";
    document.getElementById("pipelineStatusBadge").className = "status-indicator-badge completed";
    return;
  }

  state.builder.singleElIndex = eIdx;
  state.builder.singleStageIndex = sIdx;
  updateSingleElementHud();
}

function updateSingleElementHud() {
  const el = state.builder.sourceData[state.builder.singleElIndex];
  const sIdx = state.builder.singleStageIndex;

  document.getElementById("hudCurrentElement").textContent = el;

  let stageName = "Source";
  let status = "In source container";
  let statusClass = "passed";

  if (sIdx === 0) {
    stageName = "Source (" + state.builder.sourceType + ")";
    status = "Loaded from source";
  } else if (sIdx <= state.builder.intermediateOps.length) {
    const op = state.builder.intermediateOps[sIdx - 1];
    stageName = op.name;

    if (op.opKey === "filter") {
      const pass = el > 20;
      status = pass ? "✅ Passed condition" : "❌ Rejected by filter";
      statusClass = pass ? "passed" : "removed";
    } else if (op.opKey === "map") {
      const transformed = typeof el === "number" ? el * 2 : el.toUpperCase();
      status = `⚡ Transformed to ${transformed}`;
      statusClass = "transformed";
    } else {
      status = "Processed by " + op.name;
    }
  } else {
    stageName = "." + state.builder.terminalOp + "()";
    status = "Folded into terminal result";
    statusClass = "transformed";
  }

  document.getElementById("hudCurrentStage").textContent = stageName;
  const statusBadge = document.getElementById("hudCurrentStatus");
  statusBadge.textContent = status;
  statusBadge.className = `hud-status-badge ${statusClass}`;

  highlightStage(sIdx);
}

function resetPipeline() {
  state.builder.isRunning = false;
  state.builder.currentStepIndex = 0;
  state.builder.singleElIndex = 0;
  state.builder.singleStageIndex = 0;

  document.getElementById("pipelineStatusBadge").textContent = "Ready to Run";
  document.getElementById("pipelineStatusBadge").className = "status-indicator-badge";
  document.getElementById("runBtnLabel").textContent = "Run Pipeline";
  document.getElementById("prevStepBtn").disabled = true;
  document.getElementById("nextStepBtn").disabled = false;

  rebuildPipelineTrack();
  updateTypeProgression();
  document.getElementById("stepNarrativeText").innerHTML = `Click <strong>Run Pipeline</strong> to watch data tokens transform, filter, and fold into the terminal result.`;
}

// =============================================================================
// 8. TAB 2: LAZY EVALUATION LAB
// =============================================================================

function initLazyLabTab() {
  const runBtn = document.getElementById("runLazyBtn");
  const nextBtn = document.getElementById("lazyNextStepBtn");
  const resetBtn = document.getElementById("resetLazyBtn");
  const terminalToggle = document.getElementById("lazyTerminalToggle");

  terminalToggle.addEventListener("change", (e) => {
    state.lazy.hasTerminal = e.target.checked;
    const termLine = document.getElementById("lazyLineTerminal");
    const termComment = document.getElementById("lazyTerminalComment");
    if (state.lazy.hasTerminal) {
      termLine.style.opacity = "1";
      termComment.textContent = "// Terminal (Pulls first matching element!)";
    } else {
      termLine.style.opacity = "0.35";
      termComment.textContent = "// NO TERMINAL OPERATION (Lazy: ZERO WORK!)";
    }
    resetLazySimulation();
  });

  runBtn.addEventListener("click", () => {
    runFullLazySimulation();
  });

  nextBtn.addEventListener("click", () => {
    stepLazySimulation();
  });

  resetBtn.addEventListener("click", () => {
    resetLazySimulation();
  });

  resetLazySimulation();
}

function resetLazySimulation() {
  state.lazy.currentIndex = -1;
  state.lazy.isComplete = false;
  state.lazy.inspectedCount = 0;
  state.lazy.filterCount = 0;
  state.lazy.mapCount = 0;

  document.getElementById("lazyNextStepBtn").disabled = false;
  updateLazyMetrics();

  const canvas = document.getElementById("lazyTraceCanvas");
  canvas.innerHTML = `
    <div style="display: grid; grid-template-columns: 80px repeat(3, 1fr); gap: 0.5rem; font-weight: 700; font-size: 0.72rem; color: var(--text-muted); text-transform: uppercase; border-bottom: 1px solid var(--border-color); padding-bottom: 0.5rem;">
      <div>Element</div>
      <div style="text-align:center;">1. filter(n &gt; 20)</div>
      <div style="text-align:center;">2. map(n * 2)</div>
      <div style="text-align:center;">3. findFirst()</div>
    </div>
  `;

  state.lazy.elements.forEach(el => {
    const row = document.createElement("div");
    row.className = "lazy-element-row";
    row.id = `lazy-row-${el}`;
    row.innerHTML = `
      <div><span class="token">${el}</span></div>
      <div class="lazy-step-cell pending" id="lazy-filter-${el}">Pending</div>
      <div class="lazy-step-cell pending" id="lazy-map-${el}">Pending</div>
      <div class="lazy-step-cell pending" id="lazy-find-${el}">Pending</div>
    `;
    canvas.appendChild(row);
  });

  document.getElementById("lazyCalloutText").innerHTML = `Click <strong>Run Lazy Simulation</strong> to see how <code>findFirst()</code> short-circuits execution as soon as <strong>30</strong> passes through.`;
}

function updateLazyMetrics() {
  document.getElementById("lazyMetricInspected").textContent = `${state.lazy.inspectedCount} / 5`;
  document.getElementById("lazyMetricFilter").textContent = state.lazy.filterCount;
  document.getElementById("lazyMetricMap").textContent = state.lazy.mapCount;
  document.getElementById("lazyMetricShortCircuit").textContent = state.lazy.isComplete ? "YES (Short-circuited!)" : "NO";
}

function stepLazySimulation() {
  if (!state.lazy.hasTerminal) {
    alert("⚠️ Without a terminal operation, intermediate operations are 100% lazy and do ZERO work!");
    return;
  }

  if (state.lazy.isComplete) return;

  state.lazy.currentIndex++;
  const idx = state.lazy.currentIndex;
  const elements = state.lazy.elements;

  if (idx >= elements.length) {
    state.lazy.isComplete = true;
    document.getElementById("lazyNextStepBtn").disabled = true;
    return;
  }

  const el = elements[idx];
  state.lazy.inspectedCount++;
  state.lazy.filterCount++;

  const row = document.getElementById(`lazy-row-${el}`);
  if (row) row.classList.add("active");

  const filterCell = document.getElementById(`lazy-filter-${el}`);
  const mapCell = document.getElementById(`lazy-map-${el}`);
  const findCell = document.getElementById(`lazy-find-${el}`);

  const passed = el > 20;
  if (passed) {
    filterCell.className = "lazy-step-cell passed";
    filterCell.textContent = `✓ ${el} > 20`;

    // Map executes on 30
    state.lazy.mapCount++;
    const mapped = el * 2;
    mapCell.className = "lazy-step-cell passed";
    mapCell.textContent = `⚡ ${el} * 2 = ${mapped}`;

    // findFirst satisfied!
    findCell.className = "lazy-step-cell short-circuited";
    findCell.textContent = `🎯 FOUND [${mapped}]`;

    // SHORT CIRCUIT!
    state.lazy.isComplete = true;
    document.getElementById("lazyNextStepBtn").disabled = true;
    updateLazyMetrics();

    // Mark remaining elements as skipped
    for (let j = idx + 1; j < elements.length; j++) {
      const remEl = elements[j];
      const remRow = document.getElementById(`lazy-row-${remEl}`);
      if (remRow) remRow.classList.add("completed");
      document.getElementById(`lazy-filter-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-filter-${remEl}`).textContent = "Untouched";
      document.getElementById(`lazy-map-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-map-${remEl}`).textContent = "Untouched";
      document.getElementById(`lazy-find-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-find-${remEl}`).textContent = "Untouched";
    }

    document.getElementById("lazyCalloutText").innerHTML = `
      <strong>Execution stopped.</strong> Elements <strong>40</strong> and <strong>50</strong> were <strong>never processed</strong>!
      Notice that only 3 filters and 1 map were executed.
    `;
  } else {
    filterCell.className = "lazy-step-cell rejected";
    filterCell.textContent = `✗ ${el} <= 20`;

    mapCell.className = "lazy-step-cell skipped";
    mapCell.textContent = "Never called!";

    findCell.className = "lazy-step-cell skipped";
    findCell.textContent = "Never reached!";
    updateLazyMetrics();
  }
}

function runFullLazySimulation() {
  resetLazySimulation();

  if (!state.lazy.hasTerminal) {
    alert("⚠️ No terminal operation attached! Intermediate operations remain completely un-executed.");
    return;
  }

  let i = 0;
  const timer = setInterval(() => {
    if (state.lazy.isComplete || i >= state.lazy.elements.length) {
      clearInterval(timer);
      return;
    }
    stepLazySimulation();
    i++;
  }, 900);
}

// =============================================================================
// 9. TAB 3: TRADITIONAL LOOP VS STREAM COMPARISON
// =============================================================================

function initComparisonTab() {
  const animBtn = document.getElementById("runComparisonAnimBtn");
  animBtn.addEventListener("click", () => {
    runComparisonAnimation();
  });
  renderComparisonStatic();
}

function renderComparisonStatic() {
  const loopContainer = document.getElementById("loopStepsContainer");
  const streamContainer = document.getElementById("streamStepsContainer");

  loopContainer.innerHTML = `
    <div class="comp-step-item">Step 1: Check 10 &gt; 20 → false (skip)</div>
    <div class="comp-step-item">Step 2: Check 20 &gt; 20 → false (skip)</div>
    <div class="comp-step-item">Step 3: Check 30 &gt; 20 → true → calculate 30 * 2 = 60 → add to list</div>
    <div class="comp-step-item">Step 4: Check 40 &gt; 20 → true → calculate 40 * 2 = 80 → add to list</div>
    <div class="comp-step-item">Step 5: Check 50 &gt; 20 → true → calculate 50 * 2 = 100 → add to list</div>
  `;

  streamContainer.innerHTML = `
    <div class="comp-step-item">Stage 1: filter(n -&gt; n &gt; 20) pipeline established</div>
    <div class="comp-step-item">Stage 2: map(n -&gt; n * 2) pipeline chained</div>
    <div class="comp-step-item">Stage 3: toList() triggers lazy pull evaluation</div>
    <div class="comp-step-item">Result: Collected into unmodifiable List [60, 80, 100]</div>
  `;
}

function runComparisonAnimation() {
  const loopItems = document.querySelectorAll("#loopStepsContainer .comp-step-item");
  const streamItems = document.querySelectorAll("#streamStepsContainer .comp-step-item");

  loopItems.forEach(i => i.classList.remove("active"));
  streamItems.forEach(i => i.classList.remove("active"));

  let idx = 0;
  const timer = setInterval(() => {
    if (idx < loopItems.length) {
      loopItems[idx].classList.add("active");
    }
    if (idx < streamItems.length) {
      streamItems[idx].classList.add("active");
    }
    idx++;
    if (idx > Math.max(loopItems.length, streamItems.length)) {
      clearInterval(timer);
    }
  }, 600);
}

// =============================================================================
// 10. TAB 4: OPERATIONS EXPLORER (25 OPERATIONS)
// =============================================================================

function initOpsExplorerTab() {
  renderOpsCards("all");

  const filterPills = document.querySelectorAll(".filter-pill");
  filterPills.forEach(pill => {
    pill.addEventListener("click", () => {
      filterPills.forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      renderOpsCards(pill.dataset.filter);
    });
  });

  selectOperation("filter");
}

function renderOpsCards(filter) {
  const grid = document.getElementById("opsCardGrid");
  grid.innerHTML = "";

  Object.entries(OPERATIONS_CATALOG).forEach(([key, op]) => {
    if (filter === "intermediate" && op.category !== "intermediate") return;
    if (filter === "terminal" && op.category !== "terminal") return;
    if (filter === "shortCircuit" && !op.shortCircuit) return;

    const card = document.createElement("div");
    card.className = `op-card is-${op.category} ${key === state.selectedOpKey ? "selected" : ""}`;
    card.id = `op-card-${key}`;
    card.innerHTML = `
      <div class="op-card-top">
        <span class="op-card-name">${op.name}</span>
        <span class="rule-chip ${op.category}">
          ${op.category === "intermediate" ? "🟢 Inter" : "🟠 Term"}
        </span>
      </div>
      <div class="op-card-summary">${op.why}</div>
      <div class="op-card-badges-row">
        <span class="micro-badge state">${op.stateful ? "Stateful" : "Stateless"}</span>
        <span class="micro-badge sc">${op.shortCircuit ? "⚡ Short-Circuits" : "Non-Short-Circuit"}</span>
      </div>
    `;

    card.addEventListener("click", () => {
      selectOperation(key);
    });

    grid.appendChild(card);
  });
}

function selectOperation(opKey) {
  state.selectedOpKey = opKey;

  document.querySelectorAll(".op-card").forEach(c => c.classList.remove("selected"));
  const card = document.getElementById(`op-card-${opKey}`);
  if (card) card.classList.add("selected");

  const op = OPERATIONS_CATALOG[opKey];
  if (!op) return;

  const isInter = op.category === "intermediate";
  const sampleInput = op.sampleInput || [10, 20, 30, 40, 50];

  let traceHtml = "";
  let outputHtml = "";

  if (isInter && op.transform) {
    const { output, trace } = op.transform(sampleInput);
    traceHtml = trace.map(t => `
      <div style="padding: 0.25rem 0.5rem; border-left: 2px solid var(--accent-blue); margin-bottom: 0.25rem; font-family: var(--font-mono); font-size: 0.785rem;">
        <strong>${t.item}</strong> → ${t.note}
      </div>
    `).join("");

    outputHtml = output.map(o => `<div class="token token-passed">${o}</div>`).join("");
  } else {
    const termRes = op.execute ? op.execute(sampleInput) : { result: "Done", type: op.returnType };
    traceHtml = `
      <div style="padding: 0.4rem; font-family: var(--font-mono); font-size: 0.8rem; color: #ffa657;">
        Terminal operation consumes stream [${sampleInput.join(", ")}] and triggers execution.
      </div>
    `;
    outputHtml = `<div class="token token-transformed" style="font-size: 0.95rem;">${termRes.result}</div>`;
  }

  const inspector = document.getElementById("opDetailCard");
  inspector.innerHTML = `
    <div style="border-bottom: 1px solid var(--border-color); padding-bottom: 0.85rem; margin-bottom: 1rem;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem;">
        <h3 style="font-family: var(--font-mono); font-size: 1.3rem;">${op.name}</h3>
        <span class="rule-chip ${op.category}">
          ${isInter ? "🟢 Intermediate" : "🟠 Terminal"}
        </span>
      </div>
      <p style="font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 0.65rem;">
        <strong>Purpose:</strong> ${op.why}
      </p>
      <div class="source-syntax-box">
        <code>${op.syntax}</code>
      </div>
    </div>

    <!-- Metadata Matrix -->
    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.5rem; margin-bottom: 1.25rem;">
      <div class="metric-card">
        <span class="metric-num" style="font-size: 0.95rem;">${isInter ? "YES (Lazy)" : "NO (Executes)"}</span>
        <span class="metric-label">Lazy Evaluation?</span>
      </div>
      <div class="metric-card">
        <span class="metric-num" style="font-size: 0.95rem;">${op.stateful ? "Stateful" : "Stateless"}</span>
        <span class="metric-label">State Model</span>
      </div>
      <div class="metric-card">
        <span class="metric-num" style="font-size: 0.95rem; color: ${op.shortCircuit ? 'var(--terminal-color)' : 'var(--text-muted)'};">
          ${op.shortCircuit ? "YES ⚡" : "NO"}
        </span>
        <span class="metric-label">Short-Circuiting?</span>
      </div>
    </div>

    <!-- Input -> Transformation -> Output -->
    <div style="display: flex; flex-direction: column; gap: 0.75rem;">
      <div style="background: var(--bg-dark); padding: 0.75rem; border-radius: var(--radius-sm); border: 1px solid var(--border-subtle);">
        <div class="preview-label">1. Input Stream Data:</div>
        <div class="tokens-row">
          ${sampleInput.map(i => `<div class="token">${i}</div>`).join("")}
        </div>
      </div>

      <div style="text-align: center; color: var(--text-muted); font-weight: 800;">↓</div>

      <div style="background: var(--bg-dark); padding: 0.75rem; border-radius: var(--radius-sm); border: 1px solid var(--border-subtle);">
        <div class="preview-label">2. Visual Transformation (${op.name}):</div>
        ${traceHtml}
      </div>

      <div style="text-align: center; color: var(--text-muted); font-weight: 800;">↓</div>

      <div style="background: var(--bg-dark); padding: 0.75rem; border-radius: var(--radius-sm); border: 1px solid var(--border-subtle);">
        <div class="preview-label">${isInter ? "3. Output Stream Data:" : "3. Produced Final Result:"}</div>
        <div class="tokens-row">
          ${outputHtml}
        </div>
      </div>
    </div>

    <div style="margin-top: 1.25rem; padding-top: 0.85rem; border-top: 1px solid var(--border-subtle); display:flex; justify-content:space-between; align-items:center; font-size:0.8rem;">
      <span>Return Type: <strong style="font-family: var(--font-mono); color: ${isInter ? '#7ee787' : '#ffa657'};">${op.returnType}</strong></span>
      <span>Pipeline State: <strong>${isInter ? '🟢 Continues' : '🟠 Ends Stream'}</strong></span>
    </div>
  `;
}

// =============================================================================
// 11. TAB 5: 13 STREAM SOURCES
// =============================================================================

function initSourcesTab() {
  const grid = document.getElementById("sourcesGrid");
  grid.innerHTML = "";

  SOURCES_CATALOG.forEach(source => {
    const card = document.createElement("div");
    card.className = "source-card";
    card.innerHTML = `
      <div class="source-card-header">
        <h3 class="source-title">${source.name}</h3>
        <span class="source-return-badge">${source.returnType}</span>
      </div>
      <p style="font-size: 0.8rem; color: var(--text-secondary);">${source.description}</p>
      <div class="source-syntax-box">
        <code>${source.syntax}</code>
      </div>
      <div class="source-tokens-preview">
        <div class="preview-label">Sample Data:</div>
        <div class="tokens-row">
          ${source.items.map(it => `<div class="token">${it}</div>`).join("")}
        </div>
      </div>
      <button class="btn btn-outline btn-block btn-sm" style="margin-top: auto;" data-source-id="${source.id}">
        Load into Pipeline Builder →
      </button>
    `;

    card.querySelector(`[data-source-id="${source.id}"]`).addEventListener("click", () => {
      loadSourceIntoBuilder(source);
    });

    grid.appendChild(card);
  });
}

function loadSourceIntoBuilder(source) {
  state.builder.sourceData = source.items;
  state.builder.sourceType = source.returnType.replace("Stream<", "List<").replace("IntStream", "int[]").replace("LongStream", "long[]").replace("DoubleStream", "double[]");
  state.builder.sourceSyntax = source.syntax.split("\n")[0];

  switchTab("builder");
  renderBuilderSourceTokens();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

// =============================================================================
// 12. TAB 6: INTERVIEW MODE (QUIZ)
// =============================================================================

function initInterviewTab() {
  const prevBtn = document.getElementById("quizPrevBtn");
  const nextBtn = document.getElementById("quizNextBtn");

  prevBtn.addEventListener("click", () => {
    if (state.quiz.currentIndex > 0) {
      state.quiz.currentIndex--;
      renderQuizQuestion();
    }
  });

  nextBtn.addEventListener("click", () => {
    if (state.quiz.currentIndex < INTERVIEW_QUESTIONS.length - 1) {
      state.quiz.currentIndex++;
      renderQuizQuestion();
    }
  });

  renderQuizQuestion();
}

function renderQuizQuestion() {
  const qIdx = state.quiz.currentIndex;
  const q = INTERVIEW_QUESTIONS[qIdx];

  document.getElementById("quizProgressNum").textContent = `${qIdx + 1} / ${INTERVIEW_QUESTIONS.length}`;
  document.getElementById("quizCategory").textContent = q.category;
  document.getElementById("quizQuestionText").textContent = q.question;

  const optionsList = document.getElementById("quizOptionsList");
  optionsList.innerHTML = "";

  const revealBox = document.getElementById("quizRevealBox");
  revealBox.classList.add("hidden");

  q.options.forEach((optText, optIdx) => {
    const btn = document.createElement("button");
    btn.className = "quiz-option-btn";
    btn.textContent = `${String.fromCharCode(65 + optIdx)}. ${optText}`;

    btn.addEventListener("click", () => {
      handleQuizAnswer(optIdx, q);
    });

    optionsList.appendChild(btn);
  });

  document.getElementById("quizPrevBtn").disabled = (qIdx === 0);
  document.getElementById("quizNextBtn").disabled = (qIdx === INTERVIEW_QUESTIONS.length - 1);
}

function handleQuizAnswer(selectedIdx, question) {
  const options = document.querySelectorAll(".quiz-option-btn");
  options.forEach((btn, idx) => {
    btn.disabled = true;
    if (idx === question.correctIndex) {
      btn.classList.add("correct");
    } else if (idx === selectedIdx) {
      btn.classList.add("wrong");
    }
  });

  const isCorrect = selectedIdx === question.correctIndex;
  const revealBox = document.getElementById("quizRevealBox");
  revealBox.classList.remove("hidden");

  const status = document.getElementById("quizResultStatus");
  status.textContent = isCorrect ? "✅ Correct!" : "❌ Incorrect!";
  status.style.color = isCorrect ? "#7ee787" : "#ff7b72";

  document.getElementById("quizWhyText").textContent = question.why;
  document.getElementById("quizCodeExample").textContent = question.code;
}

// =============================================================================
// 13. TAB 7: PITFALLS & REMEMBER THIS
// =============================================================================

function initPitfallsTab() {
  const grid = document.getElementById("pitfallsGrid");
  grid.innerHTML = "";

  PITFALLS_CATALOG.forEach(p => {
    const card = document.createElement("div");
    card.className = "pitfall-card";
    card.innerHTML = `
      <div class="pitfall-header">
        <span class="trap-tag ${p.tag}">Pitfall</span>
        <h4>${p.title}</h4>
      </div>
      <div class="pitfall-code-row">
        <div class="code-box-mini wrong">
          <strong style="color: #ff7b72;">❌ Wrong:</strong><br>
          <code>${p.wrongCode}</code>
        </div>
        <div class="code-box-mini correct">
          <strong style="color: #7ee787;">✅ Correct:</strong><br>
          <code>${p.correctCode}</code>
        </div>
      </div>
      <p style="font-size: 0.8rem; color: var(--text-secondary); line-height: 1.5;">
        <strong>💡 Why?</strong> ${p.why}
      </p>
    `;
    grid.appendChild(card);
  });
}
