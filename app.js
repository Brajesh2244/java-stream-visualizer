/**
 * Java Stream API Visualizer - Core Application Logic
 * Implements interactive pipeline building, operation exploration,
 * lazy evaluation step simulation, and real Java code generation.
 */

// =============================================================================
// 1. DATA CATALOGS & DEFINITIONS
// =============================================================================

const SOURCES_DATA = [
  {
    id: "list-int",
    name: "List<Integer>",
    category: "Collection",
    syntax: "List<Integer> list = Arrays.asList(10, 20, 30, 40, 50);\nStream<Integer> stream = list.stream();",
    returnType: "Stream<Integer>",
    items: [10, 20, 30, 40, 50],
    description: "Most common stream source. Any Java Collection (List, Set, Queue) has a .stream() method."
  },
  {
    id: "set-string",
    name: "Set<String>",
    category: "Collection",
    syntax: 'Set<String> set = Set.of("Apple", "Banana", "Cherry", "Date");\nStream<String> stream = set.stream();',
    returnType: "Stream<String>",
    items: ["Apple", "Banana", "Cherry", "Date"],
    description: "Sets provide unordered streams with guaranteed unique elements."
  },
  {
    id: "array-obj",
    name: "Object Array (String[])",
    category: "Array",
    syntax: 'String[] languages = {"Java", "Python", "Rust", "Go"};\nStream<String> stream = Arrays.stream(languages);',
    returnType: "Stream<String>",
    items: ["Java", "Python", "Rust", "Go"],
    description: "Arrays are converted using Arrays.stream(array) or Stream.of(array)."
  },
  {
    id: "array-int",
    name: "int[] (Primitive Array)",
    category: "Primitive Stream",
    syntax: "int[] numbers = {5, 12, 18, 24, 30};\nIntStream stream = Arrays.stream(numbers);",
    returnType: "IntStream",
    items: [5, 12, 18, 24, 30],
    description: "Specialized primitive stream avoids boxing/unboxing overhead of Integer."
  },
  {
    id: "array-long",
    name: "long[] (Primitive Array)",
    category: "Primitive Stream",
    syntax: "long[] timestamps = {100L, 250L, 500L, 1000L};\nLongStream stream = Arrays.stream(timestamps);",
    returnType: "LongStream",
    items: ["100L", "250L", "500L", "1000L"],
    description: "Primitive LongStream specialized for large 64-bit numerical computations."
  },
  {
    id: "array-double",
    name: "double[] (Primitive Array)",
    category: "Primitive Stream",
    syntax: "double[] prices = {1.5, 2.8, 3.2, 4.9};\nDoubleStream stream = Arrays.stream(prices);",
    returnType: "DoubleStream",
    items: [1.5, 2.8, 3.2, 4.9],
    description: "Primitive DoubleStream specialized for floating-point calculations."
  },
  {
    id: "map-keys",
    name: "Map Keys (map.keySet())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 1, "B", 2, "C", 3);\nStream<String> stream = map.keySet().stream();',
    returnType: "Stream<String>",
    items: ["key:A", "key:B", "key:C"],
    description: "Streams through keys of a Map using .keySet().stream()."
  },
  {
    id: "map-values",
    name: "Map Values (map.values())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 10, "B", 20, "C", 30);\nStream<Integer> stream = map.values().stream();',
    returnType: "Stream<Integer>",
    items: [10, 20, 30],
    description: "Streams through values of a Map using .values().stream()."
  },
  {
    id: "map-entries",
    name: "Map Entries (map.entrySet())",
    category: "Map View",
    syntax: 'Map<String, Integer> map = Map.of("A", 1, "B", 2);\nStream<Map.Entry<String, Integer>> s = map.entrySet().stream();',
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
    description: "Static factory method to construct a stream directly from arbitrary arguments."
  }
];

// Complete Operations Metadata
const ALL_OPERATIONS = {
  // Intermediate Operations (11)
  filter: {
    name: "filter()",
    type: "intermediate",
    why: "Selects and keeps only the elements that satisfy a boolean condition (Predicate).",
    syntax: "stream.filter(n -> n > 20)",
    sampleInput: [10, 20, 30, 40, 50],
    paramDesc: "n > 20",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        const passed = typeof item === 'number' ? item > 20 : item.length > 4;
        if (passed) {
          trace.push({ item, status: "kept", note: "passed condition" });
          output.push(item);
        } else {
          trace.push({ item, status: "removed", note: "removed" });
        }
      });
      return { output, trace };
    }
  },
  map: {
    name: "map()",
    type: "intermediate",
    why: "Transforms every element into another value using a Function (1-to-1 conversion).",
    syntax: "stream.map(n -> n * 2)",
    sampleInput: [10, 20, 30, 40, 50],
    paramDesc: "n -> n * 2",
    returnType: "Stream<R>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        const transformed = typeof item === 'number' ? item * 2 : item.toUpperCase();
        trace.push({ item, status: "transformed", note: `transformed to ${transformed}` });
        output.push(transformed);
      });
      return { output, trace };
    }
  },
  flatMap: {
    name: "flatMap()",
    type: "intermediate",
    why: "Flattens nested streams/collections into a single continuous stream (1-to-many flattening).",
    syntax: "stream.flatMap(list -> list.stream())",
    sampleInput: ["A,B", "C,D", "E"],
    paramDesc: "s -> Arrays.stream(s.split(','))",
    returnType: "Stream<R>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach(item => {
        if (typeof item === 'string' && item.includes(',')) {
          const parts = item.split(',');
          parts.forEach(p => output.push(p.trim()));
          trace.push({ item, status: "transformed", note: `flattened into [${parts.join(', ')}]` });
        } else {
          output.push(item);
          trace.push({ item, status: "kept", note: "flattened element" });
        }
      });
      return { output, trace };
    }
  },
  distinct: {
    name: "distinct()",
    type: "intermediate",
    why: "Removes duplicate elements based on their equals() and hashCode() contracts.",
    syntax: "stream.distinct()",
    sampleInput: [10, 20, 20, 30, 10, 40],
    paramDesc: "None (uses equals())",
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
          trace.push({ item, status: "kept", note: "first occurrence kept" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },
  sorted: {
    name: "sorted()",
    type: "intermediate",
    why: "Arranges elements in natural ascending order (requires Comparable).",
    syntax: "stream.sorted()",
    sampleInput: [40, 10, 50, 20, 30],
    paramDesc: "Natural order",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "transformed", note: "queued for sorting" }));
      const output = [...items].sort((a, b) => (typeof a === 'number' ? a - b : String(a).localeCompare(String(b))));
      return { output, trace };
    }
  },
  sortedDesc: {
    name: "sorted(Comparator)",
    type: "intermediate",
    why: "Arranges elements according to a custom Comparator (e.g. reverseOrder()).",
    syntax: "stream.sorted(Comparator.reverseOrder())",
    sampleInput: [10, 40, 20, 50, 30],
    paramDesc: "Comparator.reverseOrder()",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "transformed", note: "custom sort applied" }));
      const output = [...items].sort((a, b) => (typeof a === 'number' ? b - a : String(b).localeCompare(String(a))));
      return { output, trace };
    }
  },
  limit: {
    name: "limit(maxSize)",
    type: "intermediate",
    why: "Truncates the stream so that it emits no more than maxSize elements (short-circuiting).",
    syntax: "stream.limit(3)",
    sampleInput: [10, 20, 30, 40, 50],
    paramDesc: "3",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach((item, idx) => {
        if (idx < 3) {
          trace.push({ item, status: "kept", note: `index ${idx} < 3 (kept)` });
          output.push(item);
        } else {
          trace.push({ item, status: "removed", note: `limit reached (skipped)` });
        }
      });
      return { output, trace };
    }
  },
  skip: {
    name: "skip(n)",
    type: "intermediate",
    why: "Discards the first n elements of the stream and passes the remaining elements.",
    syntax: "stream.skip(2)",
    sampleInput: [10, 20, 30, 40, 50],
    paramDesc: "2",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      items.forEach((item, idx) => {
        if (idx < 2) {
          trace.push({ item, status: "removed", note: `skipped index ${idx}` });
        } else {
          trace.push({ item, status: "kept", note: "retained" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },
  peek: {
    name: "peek()",
    type: "intermediate",
    why: "Performs an action on each element without altering the stream. Intended for debugging.",
    syntax: "stream.peek(System.out::println)",
    sampleInput: [10, 20, 30],
    paramDesc: "System.out::println",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = items.map(item => ({ item, status: "kept", note: `peeked: ${item}` }));
      return { output: [...items], trace };
    }
  },
  takeWhile: {
    name: "takeWhile()",
    type: "intermediate",
    why: "Takes elements as long as the predicate is true; stops at the first element that fails (Java 9+).",
    syntax: "stream.takeWhile(n -> n < 35)",
    sampleInput: [10, 20, 30, 50, 15],
    paramDesc: "n -> n < 35",
    returnType: "Stream<T>",
    transform: (items) => {
      const trace = [];
      const output = [];
      let taking = true;
      items.forEach(item => {
        if (taking && item < 35) {
          trace.push({ item, status: "kept", note: "condition met, kept" });
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
    name: "dropWhile()",
    type: "intermediate",
    why: "Drops elements as long as the predicate is true; once an element fails, keeps all remaining (Java 9+).",
    syntax: "stream.dropWhile(n -> n < 30)",
    sampleInput: [10, 20, 35, 40, 15],
    paramDesc: "n -> n < 30",
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
          trace.push({ item, status: "kept", note: "dropping stopped, kept" });
          output.push(item);
        }
      });
      return { output, trace };
    }
  },

  // Terminal Operations (14)
  toList: {
    name: "toList()",
    type: "terminal",
    why: "Collects all stream elements into an unmodifiable List (Java 16+ shortcut for Collectors.toList()).",
    syntax: "List<Integer> list = stream.toList();",
    sampleInput: [30, 40, 50],
    returnType: "List<T>",
    shortCircuit: false,
    execute: (items) => ({ result: `[${items.join(", ")}]`, type: `List<${typeof items[0] === 'number' ? 'Integer' : 'String'}>` })
  },
  collect: {
    name: "collect()",
    type: "terminal",
    why: "Performs a mutable reduction operation using a Collector (e.g. toSet, joining, groupingBy).",
    syntax: "Set<Integer> set = stream.collect(Collectors.toSet());",
    sampleInput: [30, 40, 50],
    returnType: "R (Container)",
    shortCircuit: false,
    execute: (items) => ({ result: `Set of [${[...new Set(items)].join(", ")}]`, type: "Set<T>" })
  },
  count: {
    name: "count()",
    type: "terminal",
    why: "Returns the total number of elements in the stream as a 64-bit long integer.",
    syntax: "long count = stream.count();",
    sampleInput: [30, 40, 50],
    returnType: "long",
    shortCircuit: false,
    execute: (items) => ({ result: `${items.length}L`, type: "long" })
  },
  forEach: {
    name: "forEach()",
    type: "terminal",
    why: "Performs a side-effect action for each element of this stream. Returns void.",
    syntax: "stream.forEach(System.out::println);",
    sampleInput: [10, 20, 30],
    returnType: "void",
    shortCircuit: false,
    execute: (items) => ({ result: `Printed ${items.length} elements to console`, type: "void" })
  },
  forEachOrdered: {
    name: "forEachOrdered()",
    type: "terminal",
    why: "Performs an action on each element in the strict encounter order of the stream (vital in parallel).",
    syntax: "stream.parallel().forEachOrdered(System.out::println);",
    sampleInput: [10, 20, 30],
    returnType: "void",
    shortCircuit: false,
    execute: (items) => ({ result: `Encounter-ordered execution complete`, type: "void" })
  },
  toArray: {
    name: "toArray()",
    type: "terminal",
    why: "Returns an array containing all of the elements in this stream.",
    syntax: "Integer[] arr = stream.toArray(Integer[]::new);",
    sampleInput: [10, 20, 30],
    returnType: "Object[] / T[]",
    shortCircuit: false,
    execute: (items) => ({ result: `Array {${items.join(", ")}}`, type: "T[]" })
  },
  min: {
    name: "min()",
    type: "terminal",
    why: "Returns the minimum element according to the provided Comparator, wrapped in an Optional.",
    syntax: "Optional<Integer> min = stream.min(Integer::compare);",
    sampleInput: [30, 10, 50],
    returnType: "Optional<T>",
    shortCircuit: false,
    execute: (items) => {
      if (!items.length) return { result: "Optional.empty", type: "Optional<T>" };
      const val = [...items].sort((a,b) => a - b)[0];
      return { result: `Optional[${val}]`, type: "Optional<T>" };
    }
  },
  max: {
    name: "max()",
    type: "terminal",
    why: "Returns the maximum element according to the provided Comparator, wrapped in an Optional.",
    syntax: "Optional<Integer> max = stream.max(Integer::compare);",
    sampleInput: [30, 10, 50],
    returnType: "Optional<T>",
    shortCircuit: false,
    execute: (items) => {
      if (!items.length) return { result: "Optional.empty", type: "Optional<T>" };
      const val = [...items].sort((a,b) => b - a)[0];
      return { result: `Optional[${val}]`, type: "Optional<T>" };
    }
  },
  findFirst: {
    name: "findFirst()",
    type: "terminal",
    why: "Returns an Optional describing the first element of this stream, or empty if the stream is empty.",
    syntax: "Optional<Integer> first = stream.findFirst();",
    sampleInput: [10, 20, 30],
    returnType: "Optional<T>",
    shortCircuit: true,
    execute: (items) => ({ result: items.length ? `Optional[${items[0]}]` : "Optional.empty", type: "Optional<T>" })
  },
  findAny: {
    name: "findAny()",
    type: "terminal",
    why: "Returns an Optional describing some element of the stream; ideal for maximum speed in parallel streams.",
    syntax: "Optional<Integer> any = stream.findAny();",
    sampleInput: [10, 20, 30],
    returnType: "Optional<T>",
    shortCircuit: true,
    execute: (items) => ({ result: items.length ? `Optional[${items[0]}]` : "Optional.empty", type: "Optional<T>" })
  },
  anyMatch: {
    name: "anyMatch()",
    type: "terminal",
    why: "Returns true if ANY element satisfies the given predicate. Short-circuits immediately upon first match.",
    syntax: "boolean hasLarge = stream.anyMatch(n -> n > 20);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    shortCircuit: true,
    execute: (items) => {
      const match = items.some(n => typeof n === 'number' ? n > 20 : n.length > 4);
      return { result: `${match}`, type: "boolean" };
    }
  },
  allMatch: {
    name: "allMatch()",
    type: "terminal",
    why: "Returns true if ALL elements satisfy the predicate. Short-circuits immediately upon first non-match.",
    syntax: "boolean allPositive = stream.allMatch(n -> n > 0);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    shortCircuit: true,
    execute: (items) => {
      const match = items.every(n => typeof n === 'number' ? n > 0 : n.length > 0);
      return { result: `${match}`, type: "boolean" };
    }
  },
  noneMatch: {
    name: "noneMatch()",
    type: "terminal",
    why: "Returns true if NO elements satisfy the predicate. Short-circuits upon first match.",
    syntax: "boolean noneNeg = stream.noneMatch(n -> n < 0);",
    sampleInput: [10, 20, 30, 40],
    returnType: "boolean",
    shortCircuit: true,
    execute: (items) => {
      const match = items.every(n => typeof n === 'number' ? n >= 0 : true);
      return { result: `${match}`, type: "boolean" };
    }
  },
  reduce: {
    name: "reduce()",
    type: "terminal",
    why: "Combines stream elements into a single aggregate value using an associative accumulation function.",
    syntax: "int sum = stream.reduce(0, (a, b) -> a + b);",
    sampleInput: [10, 20, 30],
    returnType: "T / Optional<T>",
    shortCircuit: false,
    execute: (items) => {
      const sum = items.reduce((acc, curr) => (typeof curr === 'number' ? acc + curr : acc + " " + curr), 0);
      return { result: `${sum}`, type: typeof items[0] === 'number' ? 'Integer' : 'String' };
    }
  }
};

// =============================================================================
// 2. APPLICATION STATE
// =============================================================================

const state = {
  activeTab: "builder",
  
  // Builder State
  builder: {
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
    stepHistory: []
  },

  // Operations Explorer Selected Op
  selectedOpKey: "filter",

  // Lazy Evaluation Lab State
  lazy: {
    elements: [10, 20, 30, 40, 50],
    currentIndex: -1,
    hasTerminal: true,
    isComplete: false
  }
};

// =============================================================================
// 3. INITIALIZATION & DOM BINDINGS
// =============================================================================

document.addEventListener("DOMContentLoaded", () => {
  initNavigation();
  initSourcesTab();
  initOpsExplorerTab();
  initBuilderTab();
  initLazyLabTab();
});

// Navigation Handling
function initNavigation() {
  const tabs = document.querySelectorAll(".nav-tab");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      const target = tab.dataset.tab;
      
      document.querySelectorAll(".tab-pane").forEach(pane => pane.classList.remove("active"));
      const targetPane = document.getElementById(`tab-${target}`);
      if (targetPane) targetPane.classList.add("active");
      state.activeTab = target;
    });
  });
}

// =============================================================================
// 4. TAB 1: BUILD YOUR STREAM (PIPELINE BUILDER)
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
  const presetsBtn = document.getElementById("loadPresetBtn");
  const presetsMenu = document.getElementById("presetsMenu");
  const copyCodeBtn = document.getElementById("copyCodeBtn");

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
    state.builder.sourceType = typeof parsed[0] === 'number' ? 'List<Integer>' : 'List<String>';
    state.builder.sourceSyntax = `List<${typeof parsed[0] === 'number' ? 'Integer' : 'String'}> customList = Arrays.asList(${parsed.map(x => typeof x === 'string' ? `"${x}"` : x).join(", ")});`;
    renderBuilderSourceBoxes();
    rebuildPipelineTrack();
    updateTypeProgression();
    updateGeneratedCode();
  });

  // Dropdown toggle for adding operations
  addOpDropdownBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    addOpMenu.classList.toggle("show");
  });

  document.addEventListener("click", () => {
    addOpMenu.classList.remove("show");
    presetsMenu.classList.remove("show");
  });

  addOpMenu.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const opKey = btn.dataset.addOp;
      addIntermediateOperation(opKey);
      addOpMenu.classList.remove("show");
    });
  });

  // Presets Menu
  presetsBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    presetsMenu.classList.toggle("show");
  });

  presetsMenu.querySelectorAll("button").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      loadPipelinePreset(btn.dataset.preset);
      presetsMenu.classList.remove("show");
    });
  });

  // Terminal operation change
  terminalSelect.addEventListener("change", (e) => {
    state.builder.terminalOp = e.target.value;
    rebuildPipelineTrack();
    updateTypeProgression();
    updateGeneratedCode();
  });

  // Control Buttons
  runBtn.addEventListener("click", () => {
    runAnimatedPipeline();
  });

  nextBtn.addEventListener("click", () => {
    stepPipeline(1);
  });

  prevBtn.addEventListener("click", () => {
    stepPipeline(-1);
  });

  resetBtn.addEventListener("click", () => {
    resetPipeline();
  });

  copyCodeBtn.addEventListener("click", () => {
    const code = document.getElementById("generatedJavaCode").innerText;
    navigator.clipboard.writeText(code).then(() => {
      copyCodeBtn.innerHTML = `✓ Copied!`;
      setTimeout(() => {
        copyCodeBtn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg> Copy`;
      }, 1500);
    });
  });

  // Initial render
  renderBuilderSourceBoxes();
  renderBuilderOpsList();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function applySourcePreset(key) {
  if (key === "numbers-default") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(10, 20, 30, 40, 50);";
  } else if (key === "numbers-mixed") {
    state.builder.sourceData = [15, 4, 28, 4, 42, 9, 33];
    state.builder.sourceType = "List<Integer>";
    state.builder.sourceSyntax = "List<Integer> numbers = Arrays.asList(15, 4, 28, 4, 42, 9, 33);";
  } else if (key === "strings-fruits") {
    state.builder.sourceData = ["apple", "banana", "kiwi", "banana", "orange"];
    state.builder.sourceType = "List<String>";
    state.builder.sourceSyntax = 'List<String> fruits = Arrays.asList("apple", "banana", "kiwi", "banana", "orange");';
  } else if (key === "primitive-ints") {
    state.builder.sourceData = [5, 12, 18, 24, 30];
    state.builder.sourceType = "int[]";
    state.builder.sourceSyntax = "int[] primitives = {5, 12, 18, 24, 30};";
  }
  renderBuilderSourceBoxes();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function renderBuilderSourceBoxes() {
  const container = document.getElementById("builderSourceBoxes");
  container.innerHTML = "";
  state.builder.sourceData.forEach(item => {
    const box = document.createElement("div");
    box.className = "data-box";
    box.textContent = item;
    container.appendChild(box);
  });
}

function renderBuilderOpsList() {
  const container = document.getElementById("builderOpsList");
  container.innerHTML = "";
  
  if (state.builder.intermediateOps.length === 0) {
    container.innerHTML = `<div class="empty-ops-hint">No intermediate operations added. Stream will flow directly to terminal operation.</div>`;
    return;
  }

  state.builder.intermediateOps.forEach((op, index) => {
    const item = document.createElement("div");
    item.className = "op-chain-item";
    item.innerHTML = `
      <div class="op-chain-item-info">
        <div class="op-chain-name">${op.name}</div>
        <div class="op-chain-desc">Intermediate &bull; Returns Stream</div>
      </div>
      <div class="op-chain-actions">
        <button class="btn-icon-subtle" title="Remove" data-remove-idx="${index}">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
        </button>
      </div>
    `;

    item.querySelector("[data-remove-idx]").addEventListener("click", () => {
      state.builder.intermediateOps.splice(index, 1);
      renderBuilderOpsList();
      rebuildPipelineTrack();
      updateTypeProgression();
      updateGeneratedCode();
    });

    container.appendChild(item);
  });
}

function addIntermediateOperation(opKey) {
  let name = `${opKey}()`;
  let param = "";
  
  if (opKey === "filter") {
    param = "n -> n > 20";
    name = "filter(n > 20)";
  } else if (opKey === "map") {
    param = "n -> n * 2";
    name = "map(n * 2)";
  } else if (opKey === "limit") {
    param = "3";
    name = "limit(3)";
  } else if (opKey === "skip") {
    param = "2";
    name = "skip(2)";
  } else if (opKey === "takeWhile") {
    param = "n -> n < 35";
    name = "takeWhile(n < 35)";
  } else if (opKey === "dropWhile") {
    param = "n -> n < 30";
    name = "dropWhile(n < 30)";
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

  renderBuilderOpsList();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

function loadPipelinePreset(presetKey) {
  if (presetKey === "even-doubled") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" },
      { id: "map-2", opKey: "map", param: "n -> n * 2", name: "map(n * 2)" },
      { id: "sorted-3", opKey: "sorted", param: "", name: "sorted()" }
    ];
    state.builder.terminalOp = "toList";
  } else if (presetKey === "word-length") {
    state.builder.sourceData = ["apple", "banana", "kiwi", "banana", "orange"];
    state.builder.intermediateOps = [
      { id: "distinct-1", opKey: "distinct", param: "", name: "distinct()" },
      { id: "filter-2", opKey: "filter", param: "s -> s.length() > 4", name: "filter(s.length() > 4)" }
    ];
    state.builder.terminalOp = "count";
  } else if (presetKey === "find-first-greater") {
    state.builder.sourceData = [15, 4, 28, 4, 42, 9, 33];
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" }
    ];
    state.builder.terminalOp = "findFirst";
  } else if (presetKey === "distinct-limit") {
    state.builder.sourceData = [15, 4, 28, 4, 42, 9, 33];
    state.builder.intermediateOps = [
      { id: "distinct-1", opKey: "distinct", param: "", name: "distinct()" },
      { id: "sorted-2", opKey: "sorted", param: "", name: "sorted()" },
      { id: "limit-3", opKey: "limit", param: "3", name: "limit(3)" }
    ];
    state.builder.terminalOp = "toList";
  } else if (presetKey === "sum-reduce") {
    state.builder.sourceData = [10, 20, 30, 40, 50];
    state.builder.intermediateOps = [
      { id: "filter-1", opKey: "filter", param: "n -> n > 20", name: "filter(n > 20)" }
    ];
    state.builder.terminalOp = "reduce";
  }

  document.getElementById("builderTerminalSelect").value = state.builder.terminalOp;
  renderBuilderSourceBoxes();
  renderBuilderOpsList();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}

/**
 * Rebuilds the visual Pipeline Track reflecting every stage
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
      <span class="stage-type-tag source">Stream Source</span>
      <div class="stage-title">${state.builder.sourceType}</div>
      <span class="stage-subtitle">Original Data Container</span>
    </div>
    <div class="stage-data-flow" id="data-flow-source">
      ${state.builder.sourceData.map(d => `<div class="data-box">${d}</div>`).join("")}
    </div>
  `;
  track.appendChild(sourceNode);

  // Arrow
  track.appendChild(createConnectorArrow());

  // 2. Stream() Creation Node
  const streamNode = document.createElement("div");
  streamNode.className = "stage-node intermediate-stage";
  streamNode.id = "stage-stream-init";
  streamNode.innerHTML = `
    <div class="stage-header">
      <span class="stage-type-tag intermediate">🟢 Intermediate (Init)</span>
      <div class="stage-title">.stream()</div>
      <span class="stage-subtitle">Produces Stream&lt;T&gt;</span>
    </div>
    <div class="stage-data-flow" id="data-flow-stream-init">
      ${state.builder.sourceData.map(d => `<div class="data-box">${d}</div>`).join("")}
    </div>
  `;
  track.appendChild(streamNode);

  // 3. Intermediate Operations Nodes
  let currentItems = [...state.builder.sourceData];
  state.builder.stepHistory = [
    { stage: "source", items: [...currentItems], note: "Initial data loaded from source." },
    { stage: "stream-init", items: [...currentItems], note: ".stream() opened data pipeline." }
  ];

  state.builder.intermediateOps.forEach((op, idx) => {
    track.appendChild(createConnectorArrow());

    const opDef = ALL_OPERATIONS[op.opKey];
    const { output, trace } = opDef ? opDef.transform(currentItems) : { output: currentItems, trace: [] };

    const opNode = document.createElement("div");
    opNode.className = "stage-node intermediate-stage";
    opNode.id = `stage-op-${idx}`;
    opNode.innerHTML = `
      <div class="stage-header">
        <span class="stage-type-tag intermediate">🟢 Intermediate #${idx + 1}</span>
        <div class="stage-title">${op.name}</div>
        <span class="stage-subtitle">Returns Stream&lt;T&gt; &bull; Continues</span>
      </div>
      <div class="stage-data-flow" id="data-flow-op-${idx}">
        ${output.map(d => `<div class="data-box">${d}</div>`).join("")}
      </div>
    `;
    track.appendChild(opNode);

    state.builder.stepHistory.push({
      stage: `op-${idx}`,
      opName: op.name,
      items: [...output],
      trace,
      note: `${op.name} executed: ${output.length} elements in stream.`
    });

    currentItems = output;
  });

  // 4. Terminal Operation Node
  track.appendChild(createConnectorArrow());

  const termKey = state.builder.terminalOp;
  const termDef = ALL_OPERATIONS[termKey] || ALL_OPERATIONS.toList;
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
    <div class="stage-data-flow" id="data-flow-terminal">
      <div class="data-box transformed">${terminalExec.result}</div>
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
      <span class="stage-subtitle">Pipeline Terminated & Data Returned</span>
    </div>
    <div class="stage-data-flow" id="data-flow-result">
      <span style="font-family: var(--font-mono); font-weight: 700; color: #58a6ff; font-size: 1.1rem;">
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
    note: `Terminal operation .${termKey}() triggered evaluation and returned ${terminalExec.result} (${terminalExec.type}). Stream is closed!`
  });
}

function createConnectorArrow() {
  const arrow = document.createElement("div");
  arrow.className = "stage-connector-arrow";
  arrow.innerHTML = `↓`;
  return arrow;
}

/**
 * Updates dynamic type chain progression:
 * List<Integer> -> Stream<Integer> -> Stream<Integer> -> List<Integer>
 */
function updateTypeProgression() {
  const container = document.getElementById("typeChainDisplay");
  container.innerHTML = "";

  const elements = [];
  elements.push({ label: state.builder.sourceType, type: "source" });
  elements.push({ label: "Stream<T>", type: "stream" });

  state.builder.intermediateOps.forEach(() => {
    elements.push({ label: "Stream<T>", type: "stream" });
  });

  const termDef = ALL_OPERATIONS[state.builder.terminalOp] || ALL_OPERATIONS.toList;
  elements.push({ label: termDef.returnType, type: "terminal" });

  elements.forEach((item, idx) => {
    const pill = document.createElement("span");
    pill.className = `type-pill ${item.type}`;
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
 * Dynamically generates idiomatic Java Code
 */
function updateGeneratedCode() {
  const codeBox = document.getElementById("generatedJavaCode");
  const termKey = state.builder.terminalOp;
  const termDef = ALL_OPERATIONS[termKey] || ALL_OPERATIONS.toList;
  
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
 * Animated step-by-step pipeline runner
 */
function runAnimatedPipeline() {
  const runBtnLabel = document.getElementById("runBtnLabel");
  const statusBadge = document.getElementById("pipelineStatusBadge");
  const prevBtn = document.getElementById("prevStepBtn");
  const nextBtn = document.getElementById("nextStepBtn");
  const speed = parseInt(document.getElementById("speedSelect").value, 10);

  state.builder.isRunning = true;
  state.builder.currentStepIndex = 0;
  statusBadge.textContent = "Running...";
  statusBadge.className = "pipeline-status-badge running";
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
      statusBadge.className = "pipeline-status-badge completed";
      runBtnLabel.textContent = "Run Pipeline";
      prevBtn.disabled = false;
      nextBtn.disabled = false;
      return;
    }

    state.builder.currentStepIndex = step;
    highlightStage(step);
  }, speed);
}

function stepPipeline(delta) {
  const newIndex = state.builder.currentStepIndex + delta;
  if (newIndex >= 0 && newIndex < state.builder.stepHistory.length) {
    state.builder.currentStepIndex = newIndex;
    highlightStage(newIndex);
  }
}

function highlightStage(stepIndex) {
  const allStages = document.querySelectorAll(".stage-node");
  allStages.forEach(s => s.classList.remove("active"));

  const stepInfo = state.builder.stepHistory[stepIndex];
  if (!stepInfo) return;

  const targetStage = document.getElementById(`stage-${stepInfo.stage}`) || allStages[stepIndex];
  if (targetStage) {
    targetStage.classList.add("active");
    targetStage.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  // Update narrative
  const narrativeText = document.getElementById("stepNarrativeText");
  narrativeText.innerHTML = `<strong>Step ${stepIndex + 1}/${state.builder.stepHistory.length}:</strong> ${stepInfo.note}`;

  // Update prev/next disabled state
  document.getElementById("prevStepBtn").disabled = (stepIndex === 0);
  document.getElementById("nextStepBtn").disabled = (stepIndex === state.builder.stepHistory.length - 1);
}

function resetPipeline() {
  state.builder.isRunning = false;
  state.builder.currentStepIndex = 0;
  document.getElementById("pipelineStatusBadge").textContent = "Ready";
  document.getElementById("pipelineStatusBadge").className = "pipeline-status-badge";
  document.getElementById("runBtnLabel").textContent = "Run Pipeline";
  document.getElementById("prevStepBtn").disabled = true;
  document.getElementById("nextStepBtn").disabled = false;
  
  rebuildPipelineTrack();
  document.getElementById("stepNarrativeText").innerHTML = `Configure your operations on the left and click <strong>Run Pipeline</strong> to watch data transform through each step.`;
}

// =============================================================================
// 5. TAB 2: OPERATIONS EXPLORER (DEEP DIVE & INSPECTOR)
// =============================================================================

function initOpsExplorerTab() {
  const grid = document.getElementById("opsCardGrid");
  const filterPills = document.querySelectorAll(".filter-pill");

  // Render cards
  renderOpsCards("all");

  filterPills.forEach(pill => {
    pill.addEventListener("click", () => {
      filterPills.forEach(p => p.classList.remove("active"));
      pill.classList.add("active");
      renderOpsCards(pill.dataset.filter);
    });
  });

  // Select default operation
  selectOperation("filter");
}

function renderOpsCards(filter) {
  const grid = document.getElementById("opsCardGrid");
  grid.innerHTML = "";

  Object.entries(ALL_OPERATIONS).forEach(([key, op]) => {
    if (filter === "intermediate" && op.type !== "intermediate") return;
    if (filter === "terminal" && op.type !== "terminal") return;

    const card = document.createElement("div");
    card.className = `op-card is-${op.type} ${key === state.selectedOpKey ? 'selected' : ''}`;
    card.id = `op-card-${key}`;
    card.innerHTML = `
      <div class="op-card-top">
        <span class="op-card-name">${op.name}</span>
        <span class="op-badge-type ${op.type === 'intermediate' ? 'inter' : 'term'}">
          ${op.type === 'intermediate' ? '🟢 Inter' : '🟠 Term'}
        </span>
      </div>
      <div class="op-card-summary">${op.why}</div>
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

  const op = ALL_OPERATIONS[opKey];
  if (!op) return;

  const inspector = document.getElementById("opDetailCard");
  const isInter = op.type === "intermediate";

  // Calculate live transformation
  const sampleInput = op.sampleInput || [10, 20, 30, 40, 50];
  let traceHtml = "";
  let outputBoxesHtml = "";

  if (isInter && op.transform) {
    const { output, trace } = op.transform(sampleInput);
    traceHtml = trace.map(t => `
      <div class="trace-item ${t.status}">
        <strong>${t.item}</strong> → ${t.note}
      </div>
    `).join("");

    outputBoxesHtml = output.map(item => `
      <div class="data-box kept">${item}</div>
    `).join("");
  } else {
    // Terminal execution preview
    const termRes = op.execute ? op.execute(sampleInput) : { result: "Done", type: op.returnType };
    traceHtml = `
      <div class="trace-item transformed">
        Consumes stream elements [${sampleInput.join(", ")}] and terminates stream!
      </div>
    `;
    outputBoxesHtml = `
      <div class="data-box transformed" style="font-size: 0.95rem; font-weight: 700;">
        Result: ${termRes.result}
      </div>
    `;
  }

  inspector.innerHTML = `
    <div class="inspector-header">
      <div class="inspector-title-row">
        <h3 class="inspector-title">${op.name}</h3>
        <span class="rule-chip ${op.type}">
          ${isInter ? '🟢 Intermediate Operation' : '🟠 Terminal Operation'}
        </span>
      </div>
      <div class="inspector-why-box">
        <strong>Why?</strong> ${op.why}
      </div>
      <div class="inspector-syntax">
        <code>${op.syntax}</code>
      </div>
    </div>

    <div class="bench-section">
      <div class="bench-title">Live Execution Flow: Input → Operation → Output</div>
      
      <div class="bench-flow">
        <!-- Input -->
        <div class="bench-step-box">
          <div class="bench-step-label">1. Input Stream Data</div>
          <div class="data-boxes-row">
            ${sampleInput.map(item => `<div class="data-box">${item}</div>`).join("")}
          </div>
        </div>

        <div class="bench-arrow">↓</div>

        <!-- Transformation Trace -->
        <div class="bench-step-box">
          <div class="bench-step-label">2. Transformation Details (${op.name})</div>
          <div class="transformation-trace-list">
            ${traceHtml}
          </div>
        </div>

        <div class="bench-arrow">↓</div>

        <!-- Output -->
        <div class="bench-step-box">
          <div class="bench-step-label">
            ${isInter ? '3. Output Stream Data' : '3. Final Produced Result (Stream Ended)'}
          </div>
          <div class="data-boxes-row">
            ${outputBoxesHtml || '<span style="color: var(--text-muted); font-size: 0.8rem;">(Empty stream)</span>'}
          </div>
        </div>
      </div>

      <div class="inspector-meta-row">
        <div>
          <span style="color: var(--text-muted);">Return Type: </span>
          <span class="inspector-return-type" style="color: ${isInter ? '#7ee787' : '#ffa657'};">
            ${op.returnType}
          </span>
        </div>
        <div>
          <span style="color: var(--text-muted);">Pipeline Status: </span>
          <strong>${isInter ? '🟢 Stream Continues' : '🟠 Stream Closed'}</strong>
        </div>
      </div>
    </div>
  `;
}

// =============================================================================
// 6. TAB 3: LAZY EVALUATION LAB
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
      termComment.textContent = "// Terminal (Triggers Pull!)";
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
  document.getElementById("lazyNextStepBtn").disabled = false;

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
      <div><span class="data-box">${el}</span></div>
      <div class="lazy-step-cell pending" id="lazy-filter-${el}">Pending</div>
      <div class="lazy-step-cell pending" id="lazy-map-${el}">Pending</div>
      <div class="lazy-step-cell pending" id="lazy-find-${el}">Pending</div>
    `;
    canvas.appendChild(row);
  });
}

function stepLazySimulation() {
  if (!state.lazy.hasTerminal) {
    alert("⚠️ Without a terminal operation (like findFirst or toList), intermediate operations are completely LAZY and will NEVER execute!");
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
  const row = document.getElementById(`lazy-row-${el}`);
  if (row) row.classList.add("active");

  const filterCell = document.getElementById(`lazy-filter-${el}`);
  const mapCell = document.getElementById(`lazy-map-${el}`);
  const findCell = document.getElementById(`lazy-find-${el}`);

  // 1. Evaluate Filter
  const passed = el > 20;
  if (passed) {
    filterCell.className = "lazy-step-cell passed";
    filterCell.textContent = `✓ ${el} > 20`;

    // 2. Evaluate Map
    const mapped = el * 2;
    mapCell.className = "lazy-step-cell passed";
    mapCell.textContent = `⚡ ${el} * 2 = ${mapped}`;

    // 3. Evaluate findFirst
    findCell.className = "lazy-step-cell short-circuited";
    findCell.textContent = `🎯 FOUND [${mapped}]`;

    // SHORT CIRCUIT!
    state.lazy.isComplete = true;
    document.getElementById("lazyNextStepBtn").disabled = true;

    // Mark remaining elements as skipped
    for (let j = idx + 1; j < elements.length; j++) {
      const remEl = elements[j];
      const remRow = document.getElementById(`lazy-row-${remEl}`);
      if (remRow) remRow.classList.add("completed");
      document.getElementById(`lazy-filter-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-filter-${remEl}`).textContent = "Skipped";
      document.getElementById(`lazy-map-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-map-${remEl}`).textContent = "Skipped";
      document.getElementById(`lazy-find-${remEl}`).className = "lazy-step-cell skipped";
      document.getElementById(`lazy-find-${remEl}`).textContent = "Skipped";
    }
  } else {
    filterCell.className = "lazy-step-cell rejected";
    filterCell.textContent = `✗ ${el} <= 20`;

    mapCell.className = "lazy-step-cell skipped";
    mapCell.textContent = "Never called!";

    findCell.className = "lazy-step-cell skipped";
    findCell.textContent = "Never reached!";
  }
}

function runFullLazySimulation() {
  resetLazySimulation();
  
  if (!state.lazy.hasTerminal) {
    alert("⚠️ No terminal operation attached! Notice that the stream stays completely un-executed. In Java, intermediate operations create the pipeline description, but execution only happens when a terminal operation is called.");
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
// 7. TAB 4: STREAM SOURCES (10 SOURCES)
// =============================================================================

function initSourcesTab() {
  const grid = document.getElementById("sourcesGrid");
  grid.innerHTML = "";

  SOURCES_DATA.forEach(source => {
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
      <div class="source-data-preview">
        <div class="source-preview-tag">Source Elements:</div>
        <div class="data-boxes-row">
          ${source.items.map(it => `<div class="data-box">${it}</div>`).join("")}
        </div>
      </div>
      <div style="margin-top: auto; padding-top: 0.5rem;">
        <button class="btn btn-outline btn-block btn-sm" data-load-source="${source.id}">
          Load into Pipeline Builder →
        </button>
      </div>
    `;

    card.querySelector(`[data-load-source="${source.id}"]`).addEventListener("click", () => {
      loadSourceIntoBuilder(source);
    });

    grid.appendChild(card);
  });
}

function loadSourceIntoBuilder(source) {
  state.builder.sourceData = source.items;
  state.builder.sourceType = source.returnType.replace("Stream<", "List<").replace("IntStream", "int[]").replace("LongStream", "long[]").replace("DoubleStream", "double[]");
  state.builder.sourceSyntax = source.syntax.split("\n")[0];

  // Switch to builder tab
  const builderTabBtn = document.querySelector('[data-tab="builder"]');
  if (builderTabBtn) builderTabBtn.click();

  renderBuilderSourceBoxes();
  rebuildPipelineTrack();
  updateTypeProgression();
  updateGeneratedCode();
}
