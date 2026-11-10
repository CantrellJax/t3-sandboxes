import { cp, mkdir, mkdtemp, open, readFile, rename, rm, writeFile } from "node:fs/promises";
import { watch } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
// node_modules/effect/dist/Pipeable.js
/**
* The `Pipeable` module defines the shared interface and implementation helpers
* for values that support Effect-style method chaining with `.pipe(...)`.
*
* A `Pipeable` value can pass itself through a sequence of unary functions from
* left to right, so code can be written as `value.pipe(f, g, h)` instead of
* deeply nesting calls. This is the method form used by many Effect data types
* to compose transformations, validations, and effectful operations while
* keeping the original value as the starting point of the pipeline.
*
* @stability stable
* @since 2.0.0
*/
/**
* Applies a `pipe` method's variadic arguments to an initial value from left
* to right.
*
* **When to use**
*
* Use to implement a custom `.pipe(...)` method from JavaScript's `arguments`
* object.
*
* **Details**
*
* This helper is intended for implementing `Pipeable.pipe` methods that
* receive JavaScript's `arguments` object. With no functions it returns the
* original value; otherwise it feeds each result into the next function.
*
* **Example** (Implementing a pipe method)
*
* ```ts import.meta.vitest
* import { Pipeable } from "effect"
*
* class NumberBox {
*   constructor(readonly value: number) {}
*
*   pipe(..._fns: ReadonlyArray<(value: number) => number>): number {
*     return Pipeable.pipeArguments(this.value, arguments) as number
*   }
* }
*
* const result = new NumberBox(5).pipe(
*   (n) => n + 2,
*   (n) => n * 3
* )
* result // => 21
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const pipeArguments = (self, args) => {
	switch (args.length) {
		case 0: return self;
		case 1: return args[0](self);
		case 2: return args[1](args[0](self));
		case 3: return args[2](args[1](args[0](self)));
		case 4: return args[3](args[2](args[1](args[0](self))));
		case 5: return args[4](args[3](args[2](args[1](args[0](self)))));
		case 6: return args[5](args[4](args[3](args[2](args[1](args[0](self))))));
		case 7: return args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))));
		case 8: return args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self))))))));
		case 9: return args[8](args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))))));
		default: {
			let ret = self;
			for (let i = 0, len = args.length; i < len; i++) ret = args[i](ret);
			return ret;
		}
	}
};
/**
* Reusable prototype that implements `Pipeable.pipe`.
*
* **When to use**
*
* Use when classes or object prototypes can reuse this value when they need the
* standard pipe implementation backed by `pipeArguments`.
*
* @stability stable
* @category prototypes
* @since 3.15.0
*/
const Prototype$1 = { pipe() {
	return pipeArguments(this, arguments);
} };
/**
* Provides a base constructor whose instances implement the standard `Pipeable.pipe`
* method.
*
* **When to use**
*
* Use when you need to define a class that supports Effect-style method
* chaining through `.pipe(...)`.
*
* @stability stable
* @category constructors
* @since 3.15.0
*/
const Class$1 = /*#__PURE__*/ function() {
	function PipeableBase() {}
	PipeableBase.prototype = Prototype$1;
	return PipeableBase;
}();
// node_modules/effect/dist/Function.js
/**
* Creates a function that can be called in data-first style or data-last
* (`pipe`-friendly) style.
*
* **When to use**
*
* Use to expose one implementation through both direct and `pipe`-friendly
* call styles.
*
* **Details**
*
* Pass either the arity of the uncurried function or a predicate that decides
* whether the current call is data-first. Arity is the common case. Use a
* predicate when optional arguments make arity ambiguous.
*
* **Example** (Selecting data-first or data-last style by arity)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum = Function.dual<
*   (that: number) => (self: number) => number,
*   (self: number, that: number) => number
* >(2, (self, that) => self + that)
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* **Example** (Defining overloads with call signatures)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum: {
*   (that: number): (self: number) => number
*   (self: number, that: number): number
* } = Function.dual(2, (self: number, that: number): number => self + that)
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* **Example** (Selecting data-first or data-last style with a predicate)
*
* ```ts import.meta.vitest
* import { Function, pipe } from "effect"
*
* const sum = Function.dual<
*   (that: number) => (self: number) => number,
*   (self: number, that: number) => number
* >(
*   (args) => args.length === 2,
*   (self, that) => self + that
* )
*
* sum(2, 3) // => 5
* pipe(2, sum(3)) // => 5
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const dual = function(arity, body) {
	if (typeof arity === "function") return function() {
		return arity(arguments) ? body.apply(this, arguments) : (self) => body(self, ...arguments);
	};
	switch (arity) {
		case 0:
		case 1: throw new RangeError(`Invalid arity ${arity}`);
		case 2: return function(a, b) {
			if (arguments.length >= 2) return body(a, b);
			return function(self) {
				return body(self, a);
			};
		};
		case 3: return function(a, b, c) {
			if (arguments.length >= 3) return body(a, b, c);
			return function(self) {
				return body(self, a, b);
			};
		};
		default: return function() {
			if (arguments.length >= arity) return body.apply(this, arguments);
			const args = arguments;
			return function(self) {
				return body(self, ...args);
			};
		};
	}
};
/**
* Returns its input argument unchanged.
*
* **When to use**
*
* Use to return a value unchanged where a function is required.
*
* **Example** (Returning the same value)
*
* ```ts import.meta.vitest
* import { identity } from "effect"
*
* identity(5) // => 5
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const identity = (a) => a;
/**
* Creates a zero-argument function that always returns the provided value.
*
* **When to use**
*
* Use when you need a thunk or callback that returns the same value on every
* invocation.
*
* **Example** (Creating a constant thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* const constNull = Function.constant(null)
*
* constNull() // => null
* constNull() // => null
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const constant = (value) => () => value;
/**
* Returns `true` when called.
*
* **When to use**
*
* Use when you need a thunk that returns `true` on every invocation.
*
* **Example** (Returning true from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constTrue() // => true
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constTrue = /*#__PURE__*/ constant(true);
/**
* Returns `false` when called.
*
* **When to use**
*
* Use when you need a thunk that returns `false` on every invocation.
*
* **Example** (Returning false from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constFalse() // => false
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constFalse = /*#__PURE__*/ constant(false);
/**
* Returns `undefined` when called.
*
* **When to use**
*
* Use when you need a thunk that returns `undefined` on every invocation.
*
* **Example** (Returning undefined from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constUndefined() // => undefined
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constUndefined = /*#__PURE__*/ constant(void 0);
/**
* Returns no meaningful value when called.
*
* **When to use**
*
* Use when you need a thunk that is called only for its effect and has no
* meaningful return value.
*
* **Example** (Returning void from a thunk)
*
* ```ts import.meta.vitest
* import { Function } from "effect"
*
* Function.constVoid() // => undefined
* ```
*
* @stability stable
* @category constants
* @since 2.0.0
*/
const constVoid = constUndefined;
/**
* Creates a memoized function whose input is an object, caching results by
* object identity.
*
* **When to use**
*
* Use to reuse the result of a synchronous computation whose output is stable
* for a given object reference.
*
* **Details**
*
* Each memoized wrapper owns a private `WeakMap` keyed by object identity.
*
* **Gotchas**
*
* `undefined` is reserved to represent a cache miss and is therefore not
* supported as a return value.
*
* Structurally equal objects do not share cache entries. If the same object is
* mutated after its first call, later calls still return the cached result for
* that reference.
*
* @stability stable
* @category caching
* @since 4.0.0
*/
function memoize(f) {
	const cache = /* @__PURE__ */ new WeakMap();
	return (a) => {
		const cached = cache.get(a);
		if (cached !== void 0) return cached;
		const result = f(a);
		cache.set(a, result);
		return result;
	};
}
/**
* Creates a memoized idempotent object transformation that caches both inputs
* and their outputs by object identity.
*
* **When to use**
*
* Use when an object transformation is idempotent and its output can be safely
* reused as a fixed point.
*
* **Details**
*
* After computing an input, the returned function caches both the input and
* the output. Calling it with either reference returns the output without
* invoking the supplied function again.
*
* **Gotchas**
*
* The returned function treats each computed output as a fixed point. If
* applying the supplied function to an output would produce an observably
* different value, this memoization changes that behavior.
*
* @see {@link memoize} for memoizing functions without an idempotence requirement
* @stability stable
* @category caching
* @since 4.0.0
*/
function memoizeIdempotent(f) {
	const cache = /* @__PURE__ */ new WeakMap();
	return (a) => {
		const cached = cache.get(a);
		if (cached !== void 0) return cached;
		const result = f(a);
		cache.set(a, result);
		if (result !== a) cache.set(result, result);
		return result;
	};
}
// node_modules/effect/dist/internal/equal.js
/** @internal */
const getAllObjectKeys = (obj) => {
	const keys = new Set(Reflect.ownKeys(obj));
	if (obj.constructor === Object) return keys;
	if (obj instanceof Error) keys.delete("stack");
	const proto = Object.getPrototypeOf(obj);
	let current = proto;
	while (current !== null && current !== Object.prototype) {
		const ownKeys = Reflect.ownKeys(current);
		for (let i = 0; i < ownKeys.length; i++) keys.add(ownKeys[i]);
		current = Object.getPrototypeOf(current);
	}
	if (keys.has("constructor") && typeof obj.constructor === "function" && proto === obj.constructor.prototype) keys.delete("constructor");
	return keys;
};
/** @internal */
const byReferenceInstances = /*#__PURE__*/ new WeakSet();
/** @internal */
const viewBytes = (view) => new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
/** @internal */
const isArrayBufferLike = (u) => u instanceof ArrayBuffer || typeof SharedArrayBuffer !== "undefined" && u instanceof SharedArrayBuffer;
/**
* Returns `undefined` for detached buffers, which can no longer be read.
*
* @internal
*/
const bufferBytes = (buffer) => {
	try {
		return new Uint8Array(buffer);
	} catch {
		return;
	}
};
// node_modules/effect/dist/internal/hash.js
/**
* Back-edge count used to avoid caching entry-point-dependent hashes.
*
* @internal
*/
let backEdges = 0;
/** @internal */
const addBackEdge = () => {
	backEdges++;
};
// node_modules/effect/dist/Predicate.js
/**
* Defines runtime checks for values.
*
* A `Predicate<A>` returns `true` or `false` for an `A`. A
* `Refinement<A, B>` is a predicate that also narrows the TypeScript type when
* it succeeds. This module includes guards for common JavaScript values,
* property and tag checks, tuple and struct checks, boolean combinators, and
* helpers for composing predicates and refinements.
*
* @stability stable
* @since 2.0.0
*/
/**
* Checks whether a value is a `string`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* string.
*
* **Details**
*
* Uses `typeof input === "string"`.
*
* **Example** (Guarding strings)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = "hi"
*
* if (Predicate.isString(data)) {
*   data.toUpperCase() // => "HI"
* }
* ```
*
* @see {@link isNumber}
* @see {@link isBoolean}
* @see {@link Refinement}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isString$1(input) {
	return typeof input === "string";
}
/**
* Checks whether a value is a `number`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* number.
*
* **Details**
*
* Uses `typeof input === "number"` and does not exclude `NaN` or `Infinity`.
*
* **Example** (Guarding numbers)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = 42
*
* if (Predicate.isNumber(data)) {
*   data + 1 // => 43
* }
* ```
*
* @see {@link isBigInt}
* @see {@link isString}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isNumber(input) {
	return typeof input === "number";
}
/**
* Checks whether a value is a `function`.
*
* **When to use**
*
* Use when you need a `Predicate` guard to narrow an `unknown` value to a
* callable function.
*
* **Details**
*
* Uses `typeof input === "function"`.
*
* **Example** (Guarding functions)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = () => 1
*
* if (Predicate.isFunction(data)) {
*   data() // => 1
* }
* ```
*
* @see {@link isObjectKeyword}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isFunction(input) {
	return typeof input === "function";
}
/**
* Checks whether a value is not `null` and not `undefined`.
*
* **When to use**
*
* Use when you need a `Predicate` refinement that filters out nullish values
* but keeps other falsy ones.
*
* **Details**
*
* Uses `input != null`.
*
* **Example** (Filtering non-nullish values)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const values = [0, null, "", undefined]
* const present = values.filter(Predicate.isNotNullish) // => [0, ""]
* ```
*
* @see {@link isNullish}
* @see {@link isNotNull}
* @see {@link isNotUndefined}
* @stability stable
* @category guards
* @since 4.0.0
*/
function isNotNullish(input) {
	return input != null;
}
/**
* Type guard that always returns `true`.
*
* **When to use**
*
* Use when you need a `Predicate` that always accepts, e.g. as a placeholder.
*
* **Example** (Matching every value)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isUnknown(123) // => true
* ```
*
* @see {@link isNever}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isUnknown(_) {
	return true;
}
/**
* Checks whether a value is a non-null object value that is not an array.
*
* **When to use**
*
* Use to narrow unknown input to a non-null, non-array object with a
* `Predicate` guard.
*
* **Details**
*
* This is a structural runtime check using `typeof input === "object"`, so it
* also accepts object instances such as `Date`, `Map`, class instances, and
* typed arrays. It excludes `null` and arrays.
*
* **Example** (Guarding objects)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isObject({ a: 1 }) // => true
* Predicate.isObject([1, 2]) // => false
* ```
*
* @see {@link isObjectOrArray}
* @see {@link isReadonlyObject}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isObject(input) {
	return typeof input === "object" && input !== null && !Array.isArray(input);
}
/**
* Checks whether a value is an `object` in the JavaScript sense (objects, arrays, functions).
*
* **When to use**
*
* Use when you need a `Predicate` guard that accepts arrays and functions as
* well as objects.
*
* **Details**
*
* Returns `true` for arrays and functions, and `false` for `null`.
*
* **Example** (Checking object keywords)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* Predicate.isObjectKeyword(() => 1) // => true
* Predicate.isObjectKeyword(null) // => false
* ```
*
* @see {@link isObject}
* @see {@link isObjectOrArray}
* @stability stable
* @category guards
* @since 4.0.0
*/
function isObjectKeyword(input) {
	return typeof input === "object" && input !== null || isFunction(input);
}
/**
* Checks whether a value has a given property key.
*
* **When to use**
*
* Use when you need a `Predicate` guard for property access on `unknown`
* values with a simple structural object check.
*
* **Details**
*
* Uses the `in` operator and `isObjectKeyword`. This does not check property
* value types.
*
* **Example** (Guarding object properties)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const hasName = Predicate.hasProperty("name")
* const data: unknown = { name: "Ada" }
*
* if (hasName(data)) {
*   data.name // => "Ada"
* }
* ```
*
* @see {@link isTagged}
* @see {@link isObjectKeyword}
* @stability stable
* @category guards
* @since 2.0.0
*/
const hasProperty = /*#__PURE__*/ dual(2, (self, property) => isObjectKeyword(self) && property in self);
/**
* Checks whether a value is an `Error`.
*
* **When to use**
*
* Use when you need a `Predicate` guard for errors caught from unknown sources.
*
* **Details**
*
* Uses `instanceof Error`.
*
* **Example** (Guarding errors)
*
* ```ts import.meta.vitest
* import { Predicate } from "effect"
*
* const data: unknown = new Error("boom")
*
* Predicate.isError(data) // => true
* ```
*
* @see {@link isUnknown}
* @stability stable
* @category guards
* @since 2.0.0
*/
function isError(input) {
	return input instanceof Error;
}
// node_modules/effect/dist/Hash.js
/**
* Computes Effect hash values and defines the interface for objects that want
* to provide their own hash implementation. Hashes are small numeric
* fingerprints used by Effect data structures to bucket values quickly; they
* are not cryptographic digests and they are not proof that two values are
* equal. The module also includes helpers for primitive, structure, array, and
* reference-based hashes, plus functions for combining and optimizing numeric
* hash values.
*
* @stability stable
* @since 2.0.0
*/
/**
* Defines the unique identifier used to identify objects that implement the Hash interface.
*
* **When to use**
*
* Use as the computed property key for the method that supplies a custom hash
* value on a `Hash` implementor.
*
* @see {@link Hash} for the interface implemented with this symbol
* @see {@link isHash} for checking whether a value implements `Hash`
* @see {@link hash} for computing hash values
*
* @stability stable
* @category symbols
* @since 2.0.0
*/
const symbol$1 = "~effect/Hash";
/**
* Computes a hash value for any given value.
*
* **When to use**
*
* Use to compute an Effect hash for primitives, collections, and hashable
* objects.
*
* **Details**
*
* This function can hash primitives (numbers, strings, booleans, etc.) as well as
* objects, arrays, and other complex data structures. It automatically handles
* different types and provides a consistent hash value for equivalent inputs.
*
* **Gotchas**
*
* Objects being hashed must be treated as immutable after their first hash
* computation. Hash results are cached, so mutating an object after hashing will
* lead to stale cached values and broken hash-based operations. For mutable
* objects, implement a custom `Hash` interface that hashes the object reference
* rather than its content.
*
* **Example** (Hashing different values)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.hash(42) === Hash.hash(42) // => true
* Hash.hash("hello") === Hash.hash("hello") // => true
* Hash.hash([1, 2, 3]) === Hash.hash([1, 2, 3]) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const hash = (self) => {
	switch (typeof self) {
		case "number": return number$1(self);
		case "bigint": return string$1(self.toString(10));
		case "string": return string$1(self);
		case "function":
		case "object": if (self === null) break;
		else if (self instanceof Date) {
			if (Number.isNaN(self.getTime())) return string$1("Invalid Date");
			return string$1(self.toISOString());
		} else if (self instanceof RegExp) return string$1(self.toString());
		else {
			if (byReferenceInstances.has(self)) return random(self);
			const cached = hashCache.get(self);
			if (cached !== void 0) return cached;
			if (visitedObjects.has(self)) {
				addBackEdge();
				return string$1("[Circular]");
			}
			visitedObjects.add(self);
			const seen = backEdges;
			let h;
			try {
				if (isHash(self)) h = self[symbol$1]();
				else if (typeof self === "function") h = random(self);
				else if (isArrayBufferLike(self)) {
					const bytes = bufferBytes(self);
					h = bytes === void 0 ? random(self) : array(bytes);
				} else if (self instanceof DataView) h = array(viewBytes(self));
				else if (Array.isArray(self) || ArrayBuffer.isView(self)) h = array(self);
				else if (self instanceof Map) h = hashMap(self);
				else if (self instanceof Set) h = hashSet(self);
				else h = structure(self);
			} finally {
				visitedObjects.delete(self);
			}
			if (seen === backEdges) hashCache.set(self, h);
			return h;
		}
	}
	return optimize(mix(string$1(String(self))));
};
/**
* Generates a random hash value for an object and caches it.
*
* **When to use**
*
* Use to hash an object by reference identity instead of structural content.
*
* **Details**
*
* This function creates a random hash value for objects that don't have their own
* hash implementation. The hash value is cached using a WeakMap, so the same object
* will always return the same hash value during its lifetime.
*
* **Example** (Hashing objects by reference)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const obj1 = { a: 1 }
* const obj2 = { a: 1 }
*
* Hash.random(obj1) === Hash.random(obj1) // => true
*
* typeof Hash.random(obj2) // => "number"
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const random = (self) => {
	if (!randomHashCache.has(self)) randomHashCache.set(self, optimize(Math.random() * 4294967296 | 0));
	return randomHashCache.get(self);
};
const mix = (h) => {
	h ^= h >>> 16;
	h = Math.imul(h, 2246822507);
	h ^= h >>> 13;
	h = Math.imul(h, 3266489909);
	return h ^ h >>> 16;
};
/**
* Combines two hash values into a single hash value.
*
* **When to use**
*
* Use to build a hash for a composite value by folding together hash values for
* its parts.
*
* **Details**
*
* Supports direct and pipeable usage. Argument order affects the result.
*
* **Example** (Combining hash values)
*
* ```ts import.meta.vitest
* import { Hash, pipe } from "effect"
*
* const hash1 = Hash.hash("hello")
* const hash2 = Hash.hash("world")
*
* const combined = Hash.combine(hash2)(hash1)
* combined === pipe(hash1, Hash.combine(hash2)) // => true
* ```
*
* @see {@link hash} for computing hash values from arbitrary inputs
* @see {@link structureKeys} for hashing selected object fields without manual combination
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const combine = /*#__PURE__*/ dual(2, (self, b) => mix(Math.imul(self, 2654435761) + Math.imul(b, 2246822507)));
/**
* Applies bit manipulation techniques to optimize a hash value.
*
* **When to use**
*
* Use to improve the bit distribution of a raw numeric hash value.
*
* **Details**
*
* This function takes a hash value and applies bitwise operations to improve
* the distribution of hash values, reducing the likelihood of collisions.
*
* **Example** (Optimizing a hash value)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.optimize(1234567890) // => 160826066
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const optimize = (n) => n & 3221225471 | n >>> 1 & 1073741824;
/**
* Checks whether a value implements the Hash interface.
*
* **When to use**
*
* Use to detect whether an unknown value provides a custom hash implementation.
*
* **Details**
*
* This function determines whether a given value has a function under the Hash
* symbol, indicating that it can provide its own hash value implementation.
* Data that merely carries the key, such as parsed JSON, is hashed as data.
*
* **Example** (Checking for Hash support)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* class MyHashable implements Hash.Hash {
*   [Hash.symbol]() {
*     return 42
*   }
* }
*
* Hash.isHash(new MyHashable()) // => true
* Hash.isHash({}) // => false
* Hash.isHash("string") // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isHash = (u) => hasProperty(u, "~effect/Hash") && typeof u["~effect/Hash"] === "function";
const float64 = /*#__PURE__*/ new DataView(/*#__PURE__*/ new ArrayBuffer(8));
/**
* Computes a hash value for a number.
*
* **When to use**
*
* Use to hash a JavaScript number with Effect's numeric hash semantics.
*
* **Details**
*
* Int32 values hash to themselves. Other numbers hash from their IEEE-754 bits,
* with a canonical representation for `NaN`.
*
* **Example** (Hashing numbers)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Number.isInteger(Hash.number(42)) // => true
* Number.isInteger(Hash.number(3.14)) // => true
* Hash.number(NaN) === Hash.number(NaN) // => true
* Hash.number(Infinity) === Hash.number(Infinity) // => true
* Hash.number(100) === Hash.number(100) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const number$1 = (n) => {
	const h = n | 0;
	if (h === n) return optimize(h);
	float64.setFloat64(0, n !== n ? NaN : n);
	return optimize(combine(float64.getInt32(0), float64.getInt32(4)));
};
/**
* Computes a hash value for a string using the djb2 algorithm.
*
* **When to use**
*
* Use when you need a string field to contribute to a custom structural hash
* implementation.
*
* **Details**
*
* This function implements a variation of the djb2 hash algorithm, which is
* known for its good distribution properties and speed. It processes each
* character of the string to produce a consistent hash value.
*
* **Example** (Hashing strings)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* Hash.string("hello") // => 181380007
* Hash.string("world") // => 164394279
* Hash.string("") // => 5381
* Hash.string("test") === Hash.string("test") // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const string$1 = (str) => {
	let h = 5381, i = str.length;
	while (i) h = h * 33 ^ str.charCodeAt(--i);
	return optimize(h);
};
/**
* Computes a hash value for an object using only the specified keys.
*
* **When to use**
*
* Use to hash an object by a selected set of property keys.
*
* **Details**
*
* This function allows you to hash an object by considering only specific keys,
* which is useful when you want to create a hash based on a subset of an object's
* properties.
*
* **Example** (Hashing selected object keys)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const person = { name: "John", age: 30, city: "New York" }
*
* const hash1 = Hash.structureKeys(person, ["name", "age"])
* const hash2 = Hash.structureKeys(person, ["name", "city"])
*
* hash1 // => -731887653
* hash2 // => 148523102
*
* const person2 = { name: "John", age: 30, city: "Boston" }
* const hash3 = Hash.structureKeys(person2, ["name", "age"])
* hash1 === hash3 // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const structureKeys = (o, keys) => {
	let h = 12289;
	for (const key of keys) h ^= combine(hash(key), hash(o[key]));
	return optimize(h);
};
/**
* Computes a structural hash for an object using Effect's object key collection.
*
* **When to use**
*
* Use to hash an object from all structural keys collected by Effect.
*
* **Details**
*
* The hash is based on the object's structural keys and their values, including
* symbol keys and relevant prototype keys for non-plain objects.
*
* **Example** (Hashing object structures)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const obj1 = { name: "John", age: 30 }
* const obj2 = { name: "Jane", age: 25 }
* const obj3 = { name: "John", age: 30 }
*
* Hash.structure(obj1) // => -731887653
* Hash.structure(obj2) // => -222100417
* Hash.structure(obj3) // => -731887653
* Hash.structure(obj1) === Hash.structure(obj3) // => true
* ```
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const structure = (o) => structureKeys(o, getAllObjectKeys(o));
const unordered = (seed, f) => (iter) => {
	let h = seed;
	for (const element of iter) h ^= f(element);
	return optimize(h);
};
/**
* Computes a hash value for an iterable by hashing all of its elements.
*
* **When to use**
*
* Use to hash the values yielded by an iterable with Effect hash semantics.
*
* **Details**
*
* Folds element hashes with {@link combine}, so order and length affect the
* result.
*
* **Gotchas**
*
* A hash is not an equality proof. Distinct inputs can still share a hash.
*
* **Example** (Hashing arrays)
*
* ```ts import.meta.vitest
* import { Hash } from "effect"
*
* const arr1 = [1, 2, 3]
* const arr2 = [1, 2, 3]
* const arr3 = [3, 2, 1]
*
* Hash.array(arr1) === Hash.array(arr2) // => true
* Hash.array(arr1) === Hash.array(arr3) // => false
* ```
*
* @see {@link hash} for the general-purpose hash dispatcher
*
* @stability stable
* @category hashing
* @since 2.0.0
*/
const array = (arr) => {
	let h = 6151;
	for (const element of arr) h = combine(h, hash(element));
	return optimize(h);
};
const hashMap = /*#__PURE__*/ unordered(/*#__PURE__*/ string$1("Map"), ([k, v]) => combine(hash(k), hash(v)));
const setSeed = /*#__PURE__*/ string$1("Set");
const hashSet = /*#__PURE__*/ unordered(setSeed, (element) => combine(setSeed, hash(element)));
const randomHashCache = /*#__PURE__*/ new WeakMap();
const hashCache = /*#__PURE__*/ new WeakMap();
const visitedObjects = /*#__PURE__*/ new WeakSet();
// node_modules/effect/dist/Equal.js
/**
* Defines the unique string identifier for the `Equal` interface.
*
* **When to use**
*
* Use when you implement custom equality and need the computed property key for
* the equality method.
*
* **Details**
*
* This is a pure constant with no allocation or side effects.
*
* **Example** (Implementing Equal on a class)
*
* ```ts import.meta.vitest
* import { Equal, Hash } from "effect"
*
* class UserId implements Equal.Equal {
*   constructor(readonly id: string) {}
*
*   [Equal.symbol](that: Equal.Equal): boolean {
*     return that instanceof UserId && this.id === that.id
*   }
*
*   [Hash.symbol](): number {
*     return Hash.string(this.id)
*   }
* }
*
* Equal.equals(new UserId("1"), new UserId("1")) // => true
* Equal.equals(new UserId("1"), new UserId("2")) // => false
* ```
*
* @see {@link Equal} — the interface that uses this symbol
* @see {@link isEqual} — type guard for `Equal` implementors
* @stability stable
* @category symbols
* @since 2.0.0
*/
const symbol = "~effect/Equal";
function equals$1() {
	if (arguments.length === 1) return (self) => compareBoth(self, arguments[0]);
	return compareBoth(arguments[0], arguments[1]);
}
function compareBoth(self, that) {
	if (self === that) return true;
	if (self == null || that == null) return false;
	const selfType = typeof self;
	if (selfType !== typeof that) return false;
	if (selfType === "number" && self !== self && that !== that) return true;
	if (selfType !== "object" && selfType !== "function") return false;
	if (byReferenceInstances.has(self) || byReferenceInstances.has(that)) return false;
	return compareObjects(self, that);
}
function compareObjects(self, that) {
	const depth = pathLeft.length;
	for (let i = depth; i-- > 0;) if (pathLeft[i] === self && pathRight[i] === that) return true;
	if (depth) return compareOnPath(self, that);
	let known = results.get(self);
	if (!known) results.set(self, known = /* @__PURE__ */ new WeakMap());
	let result = known.get(that);
	if (result === void 0) known.set(that, result = compareOnPath(self, that));
	return result;
}
function compareOnPath(self, that) {
	pathLeft.push(self);
	pathRight.push(that);
	try {
		return compareStructure(self, that);
	} finally {
		pathLeft.pop();
		pathRight.pop();
	}
}
const pathLeft = [];
const pathRight = [];
const results = /*#__PURE__*/ new WeakMap();
function compareStructure(self, that) {
	if (hash(self) !== hash(that)) return false;
	else if (self instanceof Date) {
		if (!(that instanceof Date)) return false;
		const selfTime = self.getTime();
		const thatTime = that.getTime();
		return selfTime === thatTime || Number.isNaN(selfTime) && Number.isNaN(thatTime);
	} else if (self instanceof RegExp) return that instanceof RegExp && self.toString() === that.toString();
	const bothEquals = isEqual(self);
	if (bothEquals !== isEqual(that) || typeof self === "function" && !bothEquals) return false;
	else if (bothEquals) return self[symbol](that);
	else if (Array.isArray(self)) {
		if (!Array.isArray(that) || self.length !== that.length) return false;
		return compareArrays(self, that);
	} else if (isArrayBufferLike(self)) {
		if (!isArrayBufferLike(that) || self instanceof ArrayBuffer !== that instanceof ArrayBuffer) return false;
		const selfBytes = bufferBytes(self);
		const thatBytes = bufferBytes(that);
		return selfBytes !== void 0 && thatBytes !== void 0 && compareTypedArrays(selfBytes, thatBytes);
	} else if (ArrayBuffer.isView(self)) {
		const selfIsDataView = self instanceof DataView;
		if (!ArrayBuffer.isView(that) || self.byteLength !== that.byteLength || selfIsDataView !== that instanceof DataView) return false;
		if (selfIsDataView) return compareTypedArrays(viewBytes(self), viewBytes(that));
		return compareTypedArrays(self, that);
	} else if (self instanceof Map) {
		if (!(that instanceof Map) || self.size !== that.size) return false;
		return compareHashed(self, that, entryHash, equalEntries);
	} else if (self instanceof Set) {
		if (!(that instanceof Set) || self.size !== that.size) return false;
		return compareHashed(self, that, hash, compareBoth);
	}
	return compareRecords(self, that);
}
function compareArrays(self, that) {
	for (let i = 0; i < self.length; i++) if (!compareBoth(self[i], that[i])) return false;
	return true;
}
function compareTypedArrays(self, that) {
	if (self.length !== that.length) return false;
	for (let i = 0; i < self.length; i++) if (self[i] !== that[i]) return false;
	return true;
}
function compareRecords(self, that) {
	const selfKeys = getAllObjectKeys(self);
	const thatKeys = getAllObjectKeys(that);
	if (selfKeys.size !== thatKeys.size) return false;
	for (const key of selfKeys) if (!thatKeys.has(key) || !compareBoth(self[key], that[key])) return false;
	return true;
}
function compareHashed(self, that, hashOf, equivalent) {
	const groups = /* @__PURE__ */ new Map();
	let remaining = 0;
	for (const item of that) {
		const h = hashOf(item);
		const group = groups.get(h);
		if (group) group.push(item);
		else groups.set(h, [item]);
		remaining++;
	}
	outer: for (const item of self) {
		const group = groups.get(hashOf(item));
		if (group) {
			for (let i = 0; i < group.length; i++) if (equivalent(item, group[i])) {
				group[i] = group[group.length - 1];
				group.pop();
				remaining--;
				continue outer;
			}
		}
		return false;
	}
	return remaining === 0;
}
const entryHash = (entry) => hash(entry[0]);
const equalEntries = (self, that) => compareBoth(self[0], that[0]) && compareBoth(self[1], that[1]);
/**
* Checks whether a value implements the {@link Equal} interface.
*
* **When to use**
*
* Use when you need generic utility code to distinguish `Equal` implementors
* from plain values before calling `[Equal.symbol]` directly.
*
* **Details**
*
* - Pure function, no side effects.
* - Returns `true` if and only if `u` has a function keyed by {@link symbol}.
*   Data that merely carries the key, such as parsed JSON, is not `Equal`.
* - Acts as a TypeScript type guard, narrowing the input to {@link Equal}.
*
* **Example** (Checking Equal values)
*
* ```ts import.meta.vitest
* import { Equal, Hash } from "effect"
*
* class Token implements Equal.Equal {
*   constructor(readonly value: string) {}
*   [Equal.symbol](that: Equal.Equal): boolean {
*     return that instanceof Token && this.value === that.value
*   }
*   [Hash.symbol](): number {
*     return Hash.string(this.value)
*   }
* }
*
* Equal.isEqual(new Token("abc")) // => true
* Equal.isEqual({ x: 1 }) // => false
* Equal.isEqual(42) // => false
* ```
*
* @see {@link Equal} — the interface being checked
* @see {@link symbol} — the property key that signals `Equal` support
* @stability stable
* @category guards
* @since 2.0.0
*/
const isEqual = (u) => hasProperty(u, "~effect/Equal") && typeof u["~effect/Equal"] === "function";
// node_modules/effect/dist/Redactable.js
/**
* Defines the symbol used to identify objects that implement the {@link Redactable}
* protocol.
*
* **When to use**
*
* Use as the property key when implementing the `Redactable` protocol.
*
* **Details**
*
* Add a method under this key to make an object redactable. The method receives
* the current `Context` and must return the replacement value. The symbol is
* registered globally via `Symbol.for("~effect/Redactable")`, so it is
* identical across multiple copies of the library at runtime.
*
* **Example** (Masking an API key)
*
* ```ts import.meta.vitest
* import { Context, Redactable } from "effect"
*
* class ApiKey {
*   constructor(readonly raw: string) {}
*
*   [Redactable.symbolRedactable](_ctx: Context.Context<never>) {
*     return this.raw.slice(0, 4) + "..."
*   }
* }
*
* Redactable.redact(new ApiKey("secret-key")) // => "secr..."
* ```
*
* @see {@link Redactable} for the interface this symbol belongs to
* @see {@link isRedactable} to check whether a value has this symbol
* @stability stable
* @category symbols
* @since 3.10.0
*/
const symbolRedactable = /*#__PURE__*/ Symbol.for("~effect/Redactable");
/**
* Type guard that checks whether a value implements the {@link Redactable}
* interface.
*
* **When to use**
*
* Use to narrow an unknown value before calling redaction-specific helpers.
*
* @see {@link Redactable} for the interface being checked
* @see {@link redact} to apply redaction if the value is redactable
* @stability stable
* @category guards
* @since 3.10.0
*/
const isRedactable = (u) => hasProperty(u, symbolRedactable);
/**
* Returns a redacted value if it implements {@link Redactable}, otherwise returns it
* unchanged.
*
* **When to use**
*
* Use as the general-purpose entry point for redaction when the input may
* or may not implement the redaction protocol.
*
* **Details**
*
* This function calls {@link isRedactable} and, when it returns `true`,
* delegates to {@link getRedacted}.
*
* **Gotchas**
*
* Redaction is not recursive. Nested redactable values inside the returned
* object are not automatically redacted.
*
* @see {@link isRedactable} to check before redacting
* @see {@link getRedacted} for the lower-level variant for known redactables
* @stability stable
* @category destructors
* @since 3.10.0
*/
function redact$2(u) {
	if (isRedactable(u)) return getRedacted(u);
	return u;
}
/**
* Returns the result of calling `[symbolRedactable]` on a value that is
* already known to be {@link Redactable}.
*
* **When to use**
*
* Use when you need to read the redacted representation from a value already
* verified as `Redactable`.
*
* **Details**
*
* This function reads the current fiber's `Context` from the global fiber
* reference and passes it to the redaction method.
*
* **Gotchas**
*
* If no fiber is active, an empty `Context` is passed to the redaction method.
*
* @see {@link redact} for the higher-level variant that handles non-redactable values
* @see {@link isRedactable} for the type guard to verify before calling this
* @stability stable
* @category destructors
* @since 4.0.0
*/
function getRedacted(redactable) {
	return redactable[symbolRedactable](globalThis["~effect/Fiber/currentFiber"]?.context ?? emptyContext$1);
}
/** @internal */
const currentFiberTypeId = "~effect/Fiber/currentFiber";
const emptyMap = /*#__PURE__*/ new Map();
const emptyContext$1 = {
	"~effect/Context": {},
	base: emptyMap,
	depth: 0,
	mapUnsafe: emptyMap,
	pipe() {
		return pipeArguments(this, arguments);
	}
};
// node_modules/effect/dist/Formatter.js
/**
* Formats JavaScript values into readable strings.
*
* `format` is intended for logs, diagnostics, and error messages. It handles
* primitives, objects, arrays, dates, regular expressions, maps, sets, class
* instances, errors, circular references, and redactable values. `formatJson`
* wraps JSON formatting with redaction and circular-reference handling, and the
* module also includes helpers for property keys, paths, and dates.
*
* @stability stable
* @since 4.0.0
*/
/**
* Converts any JavaScript value into a human-readable string.
*
* **When to use**
*
* Use when you need to format arbitrary JavaScript values for debugging,
* logging, or error messages.
*
* **Details**
*
* - Output is **not** valid JSON; use {@link formatJson} when you need
*   parseable JSON.
* - Handles `BigInt`, `Symbol`, `Set`, `Map`, `Date`, `RegExp`, and class
*   instances that `JSON.stringify` cannot represent.
* - Circular references are shown as `"[Circular]"` instead of throwing.
* - Failures while inspecting a value are rendered as diagnostic placeholders instead of throwing.
* - Primitives: stringified naturally (`null`, `undefined`, `123`, `true`).
*   Strings are JSON-quoted.
* - Objects with a custom `toString` (not `Object.prototype.toString`):
*   `toString()` is called unless `ignoreToString` is `true`.
* - Errors with a `cause`: formatted as `"<message> (cause: <cause>)"`.
* - Iterables (`Set`, `Map`, etc.): formatted as
*   `ClassName([...elements])`.
* - Class instances: wrapped as `ClassName({...})`.
* - `Redactable` values are automatically redacted.
* - Arrays/objects with 0–1 entries are inline; larger ones are
*   pretty-printed when `space` is set.
* - `space` — indentation unit (number of spaces, or a string like
*   `"\t"`). Defaults to `0` (compact).
* - `ignoreToString` — skip calling `toString()`. Defaults to `false`.
*
* **Example** (Formatting compact output)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* Formatter.format({ a: 1, b: [2, 3] }) // => "{\"a\":1,\"b\":[2,3]}"
* ```
*
* **Example** (Pretty-printed output)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const output = Formatter.format({ a: 1, b: [2, 3] }, { space: 2 })
* output // => "{\n  \"a\": 1,\n  \"b\": [\n    2,\n    3\n  ]\n}"
* ```
*
* **Example** (Handling circular references)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const obj: any = { name: "loop" }
* obj.self = obj
* Formatter.format(obj) // => "{\"name\":\"loop\",\"self\":[Circular]}"
* ```
*
* @see {@link formatJson}
* @see {@link Formatter}
* @stability stable
* @category formatting
* @since 2.0.0
*/
function format$1(input, options) {
	const space = options?.space ?? 0;
	const ancestors = /* @__PURE__ */ new WeakSet();
	const gap = !space ? "" : typeof space === "number" ? " ".repeat(space) : space;
	const ind = (d) => gap.repeat(d);
	const wrap = (v, body) => {
		const ctor = v?.constructor;
		return ctor && ctor !== Object.prototype.constructor && ctor.name ? `${ctor.name}(${body})` : body;
	};
	const ownKeys = (o) => {
		try {
			return Reflect.ownKeys(o);
		} catch {
			return ["[ownKeys threw]"];
		}
	};
	function recur(v, d = 0) {
		try {
			return recurUnsafe(v, d);
		} catch {
			if (typeof v === "object" && v !== null || typeof v === "function") ancestors.delete(v);
			return "[inspection threw]";
		}
	}
	function recurUnsafe(v, d = 0) {
		if (typeof v === "string") return JSON.stringify(v);
		if (typeof v === "number" || v == null || typeof v === "boolean" || typeof v === "symbol") return String(v);
		if (typeof v === "bigint") return String(v) + "n";
		if (typeof v === "object" || typeof v === "function") {
			if (ancestors.has(v)) return CIRCULAR;
			ancestors.add(v);
			let output;
			if (symbolRedactable in v) output = recur(getRedacted(v), d);
			else if (Array.isArray(v)) output = !gap || v.length <= 1 ? `[${v.map((x) => recur(x, d)).join(",")}]` : `[\n${ind(d + 1)}${v.map((x) => recur(x, d + 1)).join(",\n" + ind(d + 1))}\n${ind(d)}]`;
			else if (v instanceof Date) output = formatDate(v);
			else if (!options?.ignoreToString && hasProperty(v, "toString") && typeof v["toString"] === "function" && v["toString"] !== Object.prototype.toString && v["toString"] !== Array.prototype.toString) {
				const s = safeToString(v);
				output = v instanceof Error && v.cause !== void 0 ? `${s} (cause: ${recur(v.cause, d)})` : s;
			} else if (Symbol.iterator in v) output = `${v.constructor.name}(${recur(Array.from(v), d)})`;
			else {
				const keys = ownKeys(v);
				if (!gap || keys.length <= 1) {
					const body = `{${keys.map((k) => `${formatPropertyKey(k)}:${recur(safeGet(v, k), d)}`).join(",")}}`;
					output = wrap(v, body);
				} else {
					const body = `{\n${keys.map((k) => `${ind(d + 1)}${formatPropertyKey(k)}: ${recur(safeGet(v, k), d + 1)}`).join(",\n")}\n${ind(d)}}`;
					output = wrap(v, body);
				}
			}
			ancestors.delete(v);
			return output;
		}
		return String(v);
	}
	return recur(input, 0);
}
const CIRCULAR = "[Circular]";
/**
* @internal
*/
function formatPropertyKey(name) {
	return typeof name === "string" ? JSON.stringify(name) : String(name);
}
/**
* Formats a `Date` as an ISO 8601 string, returning `"Invalid Date"` for
* invalid dates instead of throwing.
*
* @internal
*/
function formatDate(date) {
	try {
		return date.toISOString();
	} catch {
		return "Invalid Date";
	}
}
function safeToString(input) {
	try {
		const s = input.toString();
		return typeof s === "string" ? s : String(s);
	} catch {
		return "[toString threw]";
	}
}
function safeGet(input, key) {
	try {
		return input[key];
	} catch {
		return "[property access threw]";
	}
}
/**
* Stringifies a value to JSON safely, silently dropping circular references.
*
* **When to use**
*
* Use when you need valid JSON output, unlike `format`, and the input may
* contain circular references that should be silently omitted rather than
* throwing a `TypeError`.
*
* **Details**
*
* Uses `JSON.stringify` internally with a replacer that tracks the current
* object ancestry. Circular references are replaced with `undefined`, which
* omits them from object output. `Redactable` values are automatically redacted
* before serialization. `BigInt` values are stringified with an `n` suffix.
* `Error` instances without a `toJSON` property include their enumerable
* properties plus `name` and `message`. Errors with `toJSON` keep their custom
* representation. Other values follow standard `JSON.stringify` behavior. The
* `space` parameter controls indentation and defaults to `0`.
*
* **Gotchas**
*
* When the root input is `undefined`, a symbol, or a function, `formatJson`
* returns `"null"` instead of the `undefined` returned by `JSON.stringify`.
* Nested values retain standard `JSON.stringify` behavior.
*
* **Example** (Formatting compact JSON)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* Formatter.formatJson({ name: "Alice", age: 30 }) // => "{\"name\":\"Alice\",\"age\":30}"
* ```
*
* **Example** (Handling circular references)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const obj: any = { name: "test" }
* obj.self = obj
* Formatter.formatJson(obj) // => "{\"name\":\"test\"}"
* ```
*
* **Example** (Pretty-printed JSON)
*
* ```ts import.meta.vitest
* import { Formatter } from "effect"
*
* const output = Formatter.formatJson({ name: "Alice", age: 30 }, { space: 2 })
* output // => "{\n  \"name\": \"Alice\",\n  \"age\": 30\n}"
* ```
*
* @see {@link format}
* @see {@link Formatter}
* @stability stable
* @category serialization
* @since 4.0.0
*/
function formatJson(input, options) {
	const ancestors = [];
	const sources = /* @__PURE__ */ new WeakMap();
	return JSON.stringify(input, function(key, value) {
		const original = Object.getOwnPropertyDescriptor(sources.get(this) ?? this, key)?.value;
		const redacted = hasProperty(original, symbolRedactable) ? redact$2(original) : redact$2(value);
		if (typeof redacted === "bigint") return format$1(redacted);
		if (typeof redacted !== "object" || redacted === null) return redacted;
		const current = redacted instanceof Error && !hasProperty(redacted, "toJSON") ? {
			...redacted,
			name: redacted.name,
			message: redacted.message
		} : redacted;
		while (ancestors.length > 0 && ancestors[ancestors.length - 1] !== this) ancestors.pop();
		if (ancestors.includes(redacted)) return;
		ancestors.push(redacted);
		if (current !== redacted) ancestors.push(current);
		if (!hasGetter(current) || isJsonPrimitiveWrapper(current)) return current;
		const view = redactingView(current);
		sources.set(view, current);
		ancestors.push(view);
		return view;
	}, options?.space) ?? "null";
}
function hasGetter(object) {
	for (const key of Object.getOwnPropertyNames(object)) if (Object.getOwnPropertyDescriptor(object, key)?.get !== void 0) return true;
	return false;
}
function redactingView(object) {
	const view = Array.isArray(object) ? [] : {};
	const keys = Array.isArray(object) ? Array.from({ length: object.length }, (_, i) => String(i)) : Object.keys(object);
	for (const key of keys) Object.defineProperty(view, key, {
		enumerable: true,
		get: () => {
			const value = Reflect.get(object, key);
			return Object.getOwnPropertyDescriptor(object, key)?.get !== void 0 ? redact$2(value) : value;
		}
	});
	return view;
}
function isJsonPrimitiveWrapper(object) {
	for (const valueOf of [
		Number.prototype.valueOf,
		Boolean.prototype.valueOf,
		String.prototype.valueOf
	]) try {
		Reflect.apply(valueOf, object, []);
		return true;
	} catch {}
	return false;
}
// node_modules/effect/dist/Inspectable.js
/**
* Controls how values appear in logs and debugging output.
*
* Effect data types use `Inspectable` to provide stable string, JSON, and
* Node.js inspection output. This keeps custom values readable in logs, REPLs,
* test failures, and diagnostics. This module defines the Node inspect symbol,
* the `Inspectable` interface, safe conversion helpers, and shared prototype or
* class implementations for custom values.
*
* @stability stable
* @since 2.0.0
*/
/**
* Defines the symbol used by Node.js for custom object inspection.
*
* **When to use**
*
* Use to implement Node.js custom inspection for a value.
*
* **Details**
*
* This symbol is recognized by Node.js's `util.inspect()` function and the REPL
* for custom object representation. When an object has a method with this symbol,
* it will be called to determine how the object should be displayed.
*
* **Example** (Defining custom Node inspection)
*
* ```ts import.meta.vitest
* import { Inspectable } from "effect"
*
* class CustomObject {
*   constructor(private value: string) {}
*
*   [Inspectable.NodeInspectSymbol]() {
*     return `CustomObject(${this.value})`
*   }
* }
*
* const obj = new CustomObject("hello")
* obj[Inspectable.NodeInspectSymbol]() // => "CustomObject(hello)"
* ```
*
* @stability stable
* @category symbols
* @since 2.0.0
*/
const NodeInspectSymbol = /*#__PURE__*/ Symbol.for("nodejs.util.inspect.custom");
/**
* Converts a value to its structured inspection representation.
*
* **When to use**
*
* Use when you need the structured representation of an inspectable value
* without risking unhandled errors.
*
* **Details**
*
* This function applies redaction before extracting data from objects that
* implement `toJSON`, recursively processes arrays, and handles errors
* gracefully. Plain objects are returned unchanged, so the result is not
* guaranteed to be accepted by `JSON.stringify`; it may still contain values
* such as `BigInt`, functions, or circular references.
*
* @see {@link toStringUnknown} for converting unknown values to strings
*
* @stability stable
* @category converting
* @since 4.0.0
*/
const toJson = (input) => {
	try {
		input = redact$2(input);
		if (hasProperty(input, "toJSON") && isFunction(input["toJSON"]) && input["toJSON"].length === 0) return input.toJSON();
		else if (Array.isArray(input)) return input.map(toJson);
		return input;
	} catch {
		return "[toJSON threw]";
	}
};
/**
* Converts an unknown value to a string for diagnostics.
*
* **When to use**
*
* Use to produce a diagnostic string from a value whose runtime type is unknown.
*
* **Details**
*
* Strings are returned unchanged. Objects are formatted as JSON using the
* provided whitespace setting when possible, and values that cannot be
* formatted are converted with `String`.
*
* @stability stable
* @category converting
* @since 2.0.0
*/
const toStringUnknown = (u, whitespace = 2) => {
	if (typeof u === "string") return u;
	try {
		return typeof u === "object" ? formatJson(u, { space: whitespace }) : format$1(u, { space: whitespace });
	} catch {
		return String(u);
	}
};
/**
* A base prototype object that implements the {@link Inspectable} interface.
*
* **When to use**
*
* Use as a prototype for plain objects that should share standard inspectable behavior.
*
* **Details**
*
* This object provides default implementations for the {@link Inspectable} methods.
* It can be used as a prototype for objects that want to be inspectable,
* or as a mixin to add inspection capabilities to existing objects.
*
* **Example** (Using the base inspectable prototype)
*
* ```ts import.meta.vitest
* import { Inspectable } from "effect"
*
* // Use as prototype
* const myObject = Object.create(Inspectable.BaseProto)
* myObject.name = "example"
* myObject.value = 42
*
* myObject.toString() // => "\"[toJSON threw]\""
*
* // Or extend in a constructor
* function MyClass(this: any, name: string) {
*   this.name = name
* }
* MyClass.prototype = Object.create(Inspectable.BaseProto)
* MyClass.prototype.constructor = MyClass
* ```
*
* @stability stable
* @category prototypes
* @since 2.0.0
*/
const BaseProto = {
	toJSON() {
		return toJson(this);
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	toString() {
		return format$1(this.toJSON());
	}
};
// node_modules/effect/dist/internal/stackTraceLimit.js
/**
* Check if `Error.stackTraceLimit` is writable.
* Returns `false` if the property is frozen, non-writable, or `Error` is non-extensible.
*
* @internal
*/
const isStackTraceLimitWritable = () => {
	const desc = Object.getOwnPropertyDescriptor(Error, "stackTraceLimit");
	if (desc === void 0) return Object.isExtensible(Error);
	return Object.hasOwn(desc, "writable") ? desc.writable === true : desc.set !== void 0;
};
const canWriteStackTraceLimit = /*#__PURE__*/ isStackTraceLimitWritable();
/**
* Get the current `Error.stackTraceLimit` value.
* Returns `undefined` if the property doesn't exist.
*
* @internal
*/
const getStackTraceLimit = () => Error.stackTraceLimit;
/**
* Safely set `Error.stackTraceLimit` if possible, otherwise no-op.
*
* Accepts `undefined` so a value read via {@link getStackTraceLimit} can be
* restored faithfully.
*
* @internal
*/
const setStackTraceLimit = (value) => {
	if (canWriteStackTraceLimit) Error.stackTraceLimit = value;
};
// node_modules/effect/dist/Utils.js
/**
* Yields its wrapped value exactly once, then completes with the value sent
* back in.
*
* **When to use**
*
* Use to implement `[Symbol.iterator]()` on Effect-like types so they can be
* `yield*`-ed inside generator functions, such as `Effect.gen` and
* `Option.gen`.
*
* **Details**
*
* The first call to `next()` returns a fresh `{ value: self, done: false }`.
* Every subsequent call returns `{ value: a, done: true }` where `a` is the
* argument passed to `next()`. To keep `yield*` cheap, the completion result
* is the iterator itself rather than a new object, so only the iterator and
* the single yielded result are allocated per `yield*`.
*
* **Example** (Yielding a wrapped value in a generator)
*
* ```ts import.meta.vitest
* import { Utils } from "effect"
*
* const gen = new Utils.SingleShotGen<string, number>("hello")
*
* gen.next(0).value // => "hello"
*
* gen.next(42).value // => 42
* ```
*
* @see {@link Gen} for the type-level signature that relies on `SingleShotGen`
* @stability stable
* @category constructors
* @since 2.0.0
*/
var SingleShotGen = class {
	constructor(self) {
		this.value = self;
		this.done = false;
	}
	/**
	* Yields the stored value once, then completes with the value sent back in.
	*
	* **When to use**
	*
	* Use to advance a `SingleShotGen` through its single yield and completion
	* step.
	*
	* @since 2.0.0
	*/
	next(a) {
		if (this.done) {
			this.value = a;
			return this;
		}
		this.done = true;
		return {
			value: this.value,
			done: false
		};
	}
};
const pickInternalCall = () => {
	const InternalTypeId = "~effect/Utils/internal";
	const standard = { [InternalTypeId]: (body) => {
		return body();
	} };
	const forced = { [InternalTypeId]: (body) => {
		try {
			return body();
		} finally {}
	} };
	const limit = getStackTraceLimit();
	if (limit === 0) return forced[InternalTypeId];
	setStackTraceLimit(2);
	const error = standard[InternalTypeId](() => /* @__PURE__ */ new Error());
	setStackTraceLimit(limit);
	return error.stack?.includes(InternalTypeId) === true ? standard[InternalTypeId] : forced[InternalTypeId];
};
/** @internal */
const internalCall = /*#__PURE__*/ pickInternalCall();
// node_modules/effect/dist/internal/record.js
/** @internal */
function assignProperty(self, key, value) {
	if (key === "__proto__") Object.defineProperty(self, key, {
		value,
		writable: true,
		enumerable: true,
		configurable: true
	});
	else self[key] = value;
}
/** @internal */
function assignProperties(self, source) {
	if (!Object.hasOwn(source, "__proto__")) {
		Object.assign(self, source);
		return;
	}
	for (const key of Reflect.ownKeys(source)) if (Object.prototype.propertyIsEnumerable.call(source, key)) assignProperty(self, key, source[key]);
}
// node_modules/effect/dist/internal/core.js
/** @internal */
const EffectTypeId = `~effect/Effect`;
/** @internal */
const ExitTypeId = `~effect/Exit`;
const effectVariance = {
	_A: identity,
	_E: identity,
	_R: identity
};
/** @internal */
const identifier = `${EffectTypeId}/identifier`;
/** @internal */
const args = `${EffectTypeId}/args`;
/** @internal */
const evaluate = `${EffectTypeId}/evaluate`;
/** @internal */
const contA = `${EffectTypeId}/successCont`;
/** @internal */
const contE = `${EffectTypeId}/failureCont`;
/** @internal */
const contAll = `${EffectTypeId}/ensureCont`;
/** @internal */
const Yield = /*#__PURE__*/ Symbol.for("effect/Effect/Yield");
/** @internal */
const PipeInspectableProto = {
	pipe() {
		return pipeArguments(this, arguments);
	},
	toJSON() {
		return { ...this };
	},
	toString() {
		return format$1(this.toJSON(), {
			ignoreToString: true,
			space: 2
		});
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	}
};
/** @internal */
const EffectProto = {
	[EffectTypeId]: effectVariance,
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	},
	toJSON() {
		return {
			_id: "Effect",
			op: this[identifier],
			..."~effect/Effect/args" in this ? { args: this[args] } : void 0
		};
	}
};
/** @internal */
const isEffect = (u) => hasProperty(u, EffectTypeId);
/** @internal */
const isExit = (u) => hasProperty(u, ExitTypeId);
/** @internal */
const CauseTypeId = "~effect/Cause";
/** @internal */
const CauseReasonTypeId = "~effect/Cause/Reason";
/** @internal */
const isCause = (self) => hasProperty(self, CauseTypeId);
/** @internal */
var CauseImpl = class {
	constructor(failures) {
		this[CauseTypeId] = CauseTypeId;
		this.reasons = failures;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	toJSON() {
		return {
			_id: "Cause",
			reasons: this.reasons.map((f) => f.toJSON())
		};
	}
	toString() {
		return `Cause(${format$1(this.reasons)})`;
	}
	[NodeInspectSymbol]() {
		return this.toJSON();
	}
	[symbol](that) {
		return isCause(that) && this.reasons.length === that.reasons.length && this.reasons.every((e, i) => equals$1(e, that.reasons[i]));
	}
	[symbol$1]() {
		return array(this.reasons);
	}
};
const annotationsMap = /*#__PURE__*/ new WeakMap();
/** @internal */
var ReasonBase = class {
	[CauseReasonTypeId];
	annotations;
	_tag;
	constructor(_tag, annotations, originalError) {
		this[CauseReasonTypeId] = CauseReasonTypeId;
		this._tag = _tag;
		if (annotations !== constEmptyAnnotations && typeof originalError === "object" && originalError !== null && annotations.size > 0) {
			const prevAnnotations = annotationsMap.get(originalError);
			if (prevAnnotations) annotations = new Map([...prevAnnotations, ...annotations]);
			annotationsMap.set(originalError, annotations);
		}
		this.annotations = annotations;
	}
	annotate(annotations, options) {
		if (annotations.mapUnsafe.size === 0) return this;
		const newAnnotations = new Map(this.annotations);
		annotations.mapUnsafe.forEach((value, key) => {
			if (options?.overwrite !== true && newAnnotations.has(key)) return;
			newAnnotations.set(key, value);
		});
		if (options?.overwrite !== true && newAnnotations.size === this.annotations.size) return this;
		const self = Object.assign(Object.create(Object.getPrototypeOf(this)), this);
		self.annotations = newAnnotations;
		return self;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	toString() {
		return format$1(this);
	}
	[NodeInspectSymbol]() {
		return this.toString();
	}
};
/** @internal */
const constEmptyAnnotations = /*#__PURE__*/ new Map();
/** @internal */
var Fail = class extends ReasonBase {
	constructor(error, annotations = constEmptyAnnotations) {
		super("Fail", annotations, error);
		this.error = error;
	}
	toString() {
		return `Fail(${format$1(this.error)})`;
	}
	toJSON() {
		return {
			_tag: "Fail",
			error: this.error
		};
	}
	[symbol](that) {
		return isFailReason$1(that) && equals$1(this.error, that.error) && equals$1(this.annotations, that.annotations);
	}
	[symbol$1]() {
		return combine(string$1(this._tag))(combine(hash(this.error))(hash(this.annotations)));
	}
};
/** @internal */
const causeFromReasons = (reasons) => new CauseImpl(reasons);
const dedupeReasons = (self, that) => {
	const buckets = /* @__PURE__ */ new Map();
	const out = [];
	for (const reason of self.concat(that)) {
		const hash$2 = hash(reason);
		const bucket = buckets.get(hash$2);
		if (bucket === void 0) buckets.set(hash$2, [reason]);
		else if (bucket.some((previous) => equals$1(previous, reason))) continue;
		else bucket.push(reason);
		out.push(reason);
	}
	return out;
};
/** @internal */
const causeCombine = /*#__PURE__*/ dual(2, (self, that) => {
	if (self.reasons.length === 0) return that;
	else if (that.reasons.length === 0) return self;
	const newCause = new CauseImpl(dedupeReasons(self.reasons, that.reasons));
	return equals$1(self, newCause) ? self : newCause;
});
/**
* Appends the interrupts from `interrupts` that `self` does not contain yet.
* Unlike `causeCombine`, it never calls hash or equality methods, which can be
* user-defined, random, or throwing.
*
* @internal
*/
const causeAddInterrupts = (self, interrupts) => {
	const reasons = self.reasons.slice();
	for (const reason of interrupts.reasons) if (isInterruptReason(reason) && !reasons.some((r) => isInterruptReason(r) && r.fiberId === reason.fiberId && r.annotations === reason.annotations)) reasons.push(reason);
	return reasons.length === self.reasons.length ? self : new CauseImpl(reasons);
};
/** @internal */
const causeEmpty = /*#__PURE__*/ new CauseImpl([]);
/** @internal */
const causeFail = (error) => new CauseImpl([new Fail(error)]);
/** @internal */
var Die = class extends ReasonBase {
	constructor(defect, annotations = constEmptyAnnotations) {
		super("Die", annotations, defect);
		this.defect = defect;
	}
	toString() {
		return `Die(${format$1(this.defect)})`;
	}
	toJSON() {
		return {
			_tag: "Die",
			defect: this.defect
		};
	}
	[symbol](that) {
		return isDieReason(that) && equals$1(this.defect, that.defect) && equals$1(this.annotations, that.annotations);
	}
	[symbol$1]() {
		return combine(string$1(this._tag))(combine(hash(this.defect))(hash(this.annotations)));
	}
};
/** @internal */
const causeDie = (defect) => new CauseImpl([new Die(defect)]);
/** @internal */
const causeAnnotate = /*#__PURE__*/ dual((args) => isCause(args[0]), (self, annotations, options) => {
	if (annotations.mapUnsafe.size === 0) return self;
	return new CauseImpl(self.reasons.map((f) => f.annotate(annotations, options)));
});
/** @internal */
const isFailReason$1 = (self) => self._tag === "Fail";
/** @internal */
const isDieReason = (self) => self._tag === "Die";
/** @internal */
const isInterruptReason = (self) => self._tag === "Interrupt";
function defaultEvaluate(_fiber) {
	return exitDie(`Effect.evaluate: Not implemented`);
}
/** @internal */
const makePrimitiveProto = (options) => ({
	...EffectProto,
	[identifier]: options.op,
	[evaluate]: options["~effect/Effect/evaluate"] ?? defaultEvaluate,
	[contA]: options[contA],
	[contE]: options[contE],
	[contAll]: options[contAll]
});
/** @internal */
const makePrimitive = (options) => {
	const Proto = makePrimitiveProto(options);
	const PrimitiveImpl = function(value) {
		this[args] = value;
	};
	PrimitiveImpl.prototype = Proto;
	return function(value) {
		return new PrimitiveImpl(value);
	};
};
/** @internal */
const makeExit = (options) => {
	const Proto = {
		[ExitTypeId]: ExitTypeId,
		_tag: options.op,
		get [options.prop]() {
			return this[args];
		},
		...makePrimitiveProto(options),
		toString() {
			return `${options.op}(${format$1(this[args])})`;
		},
		toJSON() {
			return {
				_id: "Exit",
				_tag: options.op,
				[options.prop]: this[args]
			};
		},
		[symbol](that) {
			return isExit(that) && that._tag === this._tag && equals$1(this["~effect/Effect/args"], that["~effect/Effect/args"]);
		},
		[symbol$1]() {
			return combine(string$1(options.op), hash(this[args]));
		}
	};
	const ExitPrimitive = function(value) {
		this[args] = value;
	};
	ExitPrimitive.prototype = Proto;
	return function(value) {
		return new ExitPrimitive(value);
	};
};
/** @internal */
const exitSucceed = /*#__PURE__*/ makeExit({
	op: "Success",
	prop: "value",
	[evaluate](fiber) {
		const cont = fiber.getCont(contA);
		return cont ? cont[contA](this[args], fiber, this) : fiber.yieldWith(this);
	}
});
/** @internal */
const StackTraceKey = { key: "effect/Cause/StackTrace" };
/** @internal */
const InterruptorStackTrace = { key: "effect/Cause/InterruptorStackTrace" };
/** @internal */
const exitFailCause = /*#__PURE__*/ makeExit({
	op: "Failure",
	prop: "cause",
	[evaluate](fiber) {
		let cause = this[args];
		let annotated = false;
		if (fiber.cache.stackFrame) {
			cause = causeAnnotate(cause, { mapUnsafe: /* @__PURE__ */ new Map([[StackTraceKey.key, fiber.cache.stackFrame]]) });
			annotated = true;
		}
		let cont = fiber.getCont(contE);
		const interruptedCause = fiber._interruptedCause;
		if (interruptedCause && fiber.interruptible) {
			let skippedHandler = false;
			while (cont && fiber.interruptible) {
				skippedHandler ||= identifier in cont;
				cont = fiber.getCont(contE);
			}
			if (skippedHandler) cause = causeFromReasons(cause.reasons.filter((reason) => reason._tag !== "Fail"));
			cause = causeAddInterrupts(cause, interruptedCause);
			annotated = true;
		}
		return cont ? cont[contE](cause, fiber, annotated ? void 0 : this) : fiber.yieldWith(annotated ? exitFailCause(cause) : this);
	}
});
/** @internal */
const exitFail = (e) => exitFailCause(causeFail(e));
/** @internal */
const exitDie = (defect) => exitFailCause(causeDie(defect));
/** @internal */
const withFiber$1 = /*#__PURE__*/ makePrimitive({
	op: "WithFiber",
	[evaluate](fiber) {
		return this[args](fiber);
	}
});
/** @internal */
const YieldableError = /*#__PURE__*/ function() {
	class YieldableError extends globalThis.Error {}
	const proto = /*#__PURE__*/ makePrimitiveProto({
		op: "YieldableError",
		[evaluate]() {
			return exitFail(this);
		}
	});
	delete proto.toString;
	Object.assign(YieldableError.prototype, proto);
	return YieldableError;
}();
/** @internal */
const Error$3 = /*#__PURE__*/ function() {
	const plainArgsSymbol = /*#__PURE__*/ Symbol.for("effect/Data/Error/plainArgs");
	return class Base extends YieldableError {
		constructor(args) {
			super(args?.message, args?.cause ? { cause: args.cause } : void 0);
			if (args) {
				assignProperties(this, args);
				Object.defineProperty(this, plainArgsSymbol, {
					value: args,
					enumerable: false
				});
			}
		}
		toJSON() {
			return {
				...this[plainArgsSymbol],
				...this
			};
		}
	};
}();
/** @internal */
const TaggedError$2 = (tag) => {
	class Base extends Error$3 {
		_tag = tag;
	}
	Base.prototype.name = tag;
	return Base;
};
/** @internal */
const DoneTypeId = "~effect/Cause/Done";
/** @internal */
const isDone$1 = (u) => hasProperty(u, DoneTypeId);
const DoneVoid = {
	[DoneTypeId]: DoneTypeId,
	_tag: "Done",
	value: void 0
};
/** @internal */
const Done = (value) => {
	if (value === void 0) return DoneVoid;
	return {
		[DoneTypeId]: DoneTypeId,
		_tag: "Done",
		value
	};
};
const doneVoid = /*#__PURE__*/ exitFail(DoneVoid);
/** @internal */
const done$2 = (value) => {
	if (value === void 0) return doneVoid;
	return exitFail(Done(value));
};
// node_modules/effect/dist/Effectable.js
/**
* Create a low-level `Effect` prototype.
*
* **When to use**
*
* Use when you need to create a custom Effect-like value without extending a
* class, by providing a label and an evaluate function that receives the
* current fiber.
*
* **Details**
*
* When the effect is evaluated, it calls `evaluate` with the current fiber.
*
* @see {@link Class} for a class-based approach to defining custom Effect values
* @see {@link Mixin} for wrapping an existing class constructor
*
* @stability stable
* @category prototypes
* @since 4.0.0
*/
const Prototype = (options) => makePrimitiveProto({
	op: options.label,
	[evaluate]: options.evaluate
});
// node_modules/effect/dist/internal/option.js
/**
* @since 2.0.0
*/
const TypeId$23 = "~effect/Option";
const CommonProto$1 = {
	[TypeId$23]: { _A: (_) => _ },
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	}
};
const SomeProto = /*#__PURE__*/ Object.defineProperty(/*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto$1), {
	_tag: "Some",
	_op: "Some",
	[symbol](that) {
		return isOption(that) && isSome$1(that) && equals$1(this.value, that.value);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.value));
	},
	toString() {
		return `some(${format$1(this.value)})`;
	},
	toJSON() {
		return {
			_id: "Option",
			_tag: this._tag,
			value: toJson(this.value)
		};
	}
}), "valueOrUndefined", { get() {
	return this.value;
} });
const NoneHash = /*#__PURE__*/ hash("None");
const NoneProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto$1), {
	_tag: "None",
	_op: "None",
	valueOrUndefined: void 0,
	[symbol](that) {
		return isOption(that) && isNone$1(that);
	},
	[symbol$1]() {
		return NoneHash;
	},
	toString() {
		return `none()`;
	},
	toJSON() {
		return {
			_id: "Option",
			_tag: this._tag
		};
	}
});
/** @internal */
const isOption = (input) => hasProperty(input, TypeId$23);
/** @internal */
const isNone$1 = (fa) => fa._tag === "None";
/** @internal */
const isSome$1 = (fa) => fa._tag === "Some";
/** @internal */
const none$1 = /*#__PURE__*/ Object.create(NoneProto);
/** @internal */
const SomeImpl = function(value) {
	this.value = value;
};
SomeImpl.prototype = SomeProto;
/** @internal */
const some$1 = (value) => new SomeImpl(value);
// node_modules/effect/dist/internal/result.js
const TypeId$22 = "~effect/Result";
const CommonProto = {
	[TypeId$22]: {
		/* v8 ignore next 2 */
		_A: (_) => _,
		_E: (_) => _
	},
	...PipeInspectableProto,
	[Symbol.iterator]() {
		return new SingleShotGen(this);
	}
};
const SuccessProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto), {
	_tag: "Success",
	_op: "Success",
	[symbol](that) {
		return isResult(that) && isSuccess$2(that) && equals$1(this.success, that.success);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.success));
	},
	toString() {
		return `success(${format$1(this.success)})`;
	},
	toJSON() {
		return {
			_id: "Result",
			_tag: this._tag,
			value: toJson(this.success)
		};
	}
});
const FailureProto = /*#__PURE__*/ Object.assign(/*#__PURE__*/ Object.create(CommonProto), {
	_tag: "Failure",
	_op: "Failure",
	[symbol](that) {
		return isResult(that) && isFailure$1(that) && equals$1(this.failure, that.failure);
	},
	[symbol$1]() {
		return combine(hash(this._tag))(hash(this.failure));
	},
	toString() {
		return `failure(${format$1(this.failure)})`;
	},
	toJSON() {
		return {
			_id: "Result",
			_tag: this._tag,
			failure: toJson(this.failure)
		};
	}
});
/** @internal */
const isResult = (input) => hasProperty(input, TypeId$22);
/** @internal */
const isFailure$1 = (result) => result._tag === "Failure";
/** @internal */
const isSuccess$2 = (result) => result._tag === "Success";
/** @internal */
const FailureImpl = function(failure) {
	this.failure = failure;
};
FailureImpl.prototype = FailureProto;
/** @internal */
const fail$5 = (failure) => new FailureImpl(failure);
/** @internal */
const SuccessImpl = function(success) {
	this.success = success;
};
SuccessImpl.prototype = SuccessProto;
/** @internal */
const succeed$5 = (success) => new SuccessImpl(success);
// node_modules/effect/dist/Order.js
/**
* Defines comparison functions for ordered values.
*
* An `Order<A>` compares two `A` values and returns whether the first is less
* than, equal to, or greater than the second. Orders are used for sorting,
* choosing minimum or maximum values, checking ranges, and building ordered data
* structures. This module includes built-in orders, constructors for custom
* orders, tools for reversing and combining comparisons, tuple and struct
* helpers, comparison predicates, clamping, and reducer support.
*
* @stability stable
* @since 2.0.0
*/
/**
* Creates a new `Order` instance from a comparison function.
*
* **When to use**
*
* Use when you need a sorting rule not covered by the built-in orders or input
* mapping helpers, and you can provide a total comparison.
*
* **Details**
*
* Uses reference equality (`===`) as a shortcut: if `self === that`, it returns
* `0` without calling the comparison function. The comparison function should
* return `-1`, `0`, or `1`, and the returned order satisfies total ordering
* laws when the comparison function does.
*
* **Example** (Creating an Order)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const byAge = Order.make<{ name: string; age: number }>((self, that) => {
*   if (self.age < that.age) return -1
*   if (self.age > that.age) return 1
*   return 0
* })
*
* byAge({ name: "Alice", age: 30 }, { name: "Bob", age: 25 }) // => 1
* byAge({ name: "Alice", age: 25 }, { name: "Bob", age: 30 }) // => -1
* ```
*
* @see {@link mapInput} to transform an order by mapping the input type
* @see {@link combine} to combine multiple orders
* @stability stable
* @category constructors
* @since 2.0.0
*/
function make$14(compare) {
	return (self, that) => self === that ? 0 : compare(self, that);
}
/**
* Order instance for strings that compares them lexicographically using JavaScript's `<` operator.
*
* **When to use**
*
* Use when you need lexicographic string ordering.
*
* **Details**
*
* Uses lexicographic dictionary ordering. The empty string is less than any
* non-empty string, and comparisons are case-sensitive.
*
* **Example** (Ordering strings)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* Order.String("apple", "banana") // => -1
* Order.String("banana", "apple") // => 1
* Order.String("apple", "apple") // => 0
* ```
*
* @see {@link mapInput} to compare objects by a string property
* @see {@link Struct} to combine with other orders for struct comparison
* @stability stable
* @category instances
* @since 4.0.0
*/
const String$4 = /*#__PURE__*/ make$14((self, that) => self < that ? -1 : 1);
/**
* Order instance for numbers that compares them numerically.
*
* **When to use**
*
* Use when you need numeric ordering for numbers.
*
* **Details**
*
* `0` is considered equal to `-0`. All `NaN` values are considered equal to
* each other, and any `NaN` is considered less than any non-`NaN` number. All
* other values use standard numeric comparison.
*
* **Example** (Ordering numbers)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* Order.Number(1, 1) // => 0
* Order.Number(1, 2) // => -1
* Order.Number(2, 1) // => 1
*
* Order.Number(0, -0) // => 0
* Order.Number(NaN, 1) // => -1
* ```
*
* @see {@link mapInput} to compare objects by a number property
* @see {@link BigInt} for bigint comparisons
* @stability stable
* @category instances
* @since 4.0.0
*/
const Number$4 = /*#__PURE__*/ make$14((self, that) => {
	if (globalThis.Number.isNaN(self) && globalThis.Number.isNaN(that)) return 0;
	if (globalThis.Number.isNaN(self)) return -1;
	if (globalThis.Number.isNaN(that)) return 1;
	return self < that ? -1 : 1;
});
/**
* Transforms an `Order` on type `A` into an `Order` on type `B` by providing a function that
* maps values of type `B` to values of type `A`.
*
* **When to use**
*
* Use when you need to adapt an `Order` to compare a larger value by one
* derived property.
*
* **Details**
*
* Applies the mapping function to both values before comparison. The mapping
* function should be pure and not have side effects so the ordering properties
* of the original order are preserved.
*
* **Example** (Mapping Input)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const byLength = Order.mapInput(Order.Number, (s: string) => s.length)
*
* byLength("a", "bb") // => -1
* byLength("bb", "a") // => 1
* byLength("aa", "bb") // => 0
* ```
*
* @see {@link combine} to combine mapped orders for multi-criteria comparison
* @see {@link Struct} to create orders for structs with multiple fields
* @stability stable
* @category mapping
* @since 2.0.0
*/
const mapInput = /*#__PURE__*/ dual(2, (self, f) => make$14((b1, b2) => self(f(b1), f(b2))));
/**
* Checks whether one value is strictly greater than another according to the given order.
*
* **When to use**
*
* Use when you need a boolean greater-than predicate using an `Order`.
*
* **Details**
*
* Returns `true` if the order returns `1`, meaning the first value is greater
* than the second. Equal or lesser values return `false`.
*
* **Example** (Checking greater-than comparisons)
*
* ```ts import.meta.vitest
* import { Order } from "effect"
*
* const isGreaterThanNumber = Order.isGreaterThan(Order.Number)
*
* isGreaterThanNumber(2, 1) // => true
* isGreaterThanNumber(1, 2) // => false
* isGreaterThanNumber(1, 1) // => false
* ```
*
* @see {@link isGreaterThanOrEqualTo} for non-strict greater than or equal
* @see {@link isLessThan} for strict less than
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isGreaterThan$1 = (O) => dual(2, (self, that) => O(self, that) === 1);
// node_modules/effect/dist/Option.js
/**
* Creates an `Option` representing the absence of a value.
*
* **When to use**
*
* Use to represent a missing or uninitialized value, such as returning "no
* result" from a function.
*
* **Details**
*
* - Returns `Option<never>`, which is a subtype of `Option<A>` for any `A`
* - Always returns the same singleton instance
*
* **Example** (Creating an empty Option)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* //      ┌─── Option<never>
* //      ▼
* const noValue = Option.none() // => Option.none()
* ```
*
* @see {@link some} for the opposite operation.
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const none = () => none$1;
/**
* Wraps the given value into an `Option` to represent its presence.
*
* **When to use**
*
* Use to wrap a known present value as `Option`
* - Returning a successful result from a partial function
*
* **Details**
*
* - Always returns `Some<A>`
* - Does not filter `null` or `undefined`; use {@link fromNullishOr} for that
*
* **Example** (Wrapping a value)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* //      ┌─── Option<number>
* //      ▼
* const value = Option.some(1) // => Option.some(1)
* ```
*
* @see {@link none} for the opposite operation.
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const some = some$1;
/**
* Checks whether an `Option` contains a value (`Some`).
*
* **When to use**
*
* Use when you need to branch on a present `Option` before accessing `.value`.
*
* **Details**
*
* - Acts as a type guard, narrowing to `Some<A>`
*
* **Example** (Checking for Some)
*
* ```ts import.meta.vitest
* import { Option } from "effect"
*
* Option.isSome(Option.some(1)) // => true
* Option.isSome(Option.none()) // => false
* ```
*
* @see {@link isNone} for the opposite check.
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isSome = isSome$1;
// node_modules/effect/dist/Context.js
/**
* Runtime type identifier attached to `Context` service keys and used by
* `isKey` to recognize them.
*
* @stability stable
* @category type IDs
* @since 4.0.0
*/
const ServiceTypeId = "~effect/Context/Service";
/**
* Creates a `Context` service key.
*
* **When to use**
*
* Use when you need to define a context service key for a dependency that must
* be provided by the surrounding context.
*
* **Details**
*
* Call `Context.Service("Key")` for a function-style key, or use the two-stage
* form `Context.Service<Self, Shape>()("Key")` for class-style service
* declarations. The returned key can be yielded as an Effect and passed to
* `Context.make`, `Context.add`, and the Context getter functions.
*
* **Gotchas**
*
* The string key is the runtime identity of the service. Reusing the same key
* string for unrelated services makes them occupy the same slot in a
* `Context`.
*
* **Example** (Creating service keys)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a simple service
* const Database = Context.Service<{
*   query: (sql: string) => string
* }>("Database")
*
* // Create a service class
* class Config extends Context.Service<Config, {
*   port: number
* }>()("Config") {}
*
* // Use the services to create contexts
* const db = Context.make(Database, {
*   query: (sql) => `Result: ${sql}`
* })
* const config = Context.make(Config, { port: 8080 })
* Context.get(db, Database).query("SELECT 1") // => "Result: SELECT 1"
* Context.get(config, Config).port // => 8080
* ```
*
* @see {@link Reference} for service keys with default values
*
* @stability stable
* @category services
* @since 4.0.0
*/
const Service = function() {
	function KeyClass() {}
	const self = KeyClass;
	Object.setPrototypeOf(self, ServiceProto);
	const init = (key, options) => {
		self.key = key;
		if (options?.defaultValue) {
			self[ReferenceTypeId] = ReferenceTypeId;
			self.defaultValue = options.defaultValue;
		}
		if (options?.make) self.make = options.make;
		if (options?.fiberCached) cacheKeys.add(key);
		return self;
	};
	return arguments.length > 0 ? init(arguments[0], arguments[1]) : init;
};
const ServiceProto = {
	[ServiceTypeId]: ServiceTypeId,
	.../*#__PURE__*/ Prototype({
		label: "Service",
		evaluate(fiber) {
			return exitSucceed(get(fiber.context, this));
		}
	}),
	toJSON() {
		return {
			_id: "Service",
			key: this.key
		};
	},
	of(self) {
		return self;
	},
	context(self) {
		return make$13(this, self);
	},
	use(f) {
		return withFiber$1((fiber) => f(get(fiber.context, this)));
	},
	useSync(f) {
		return withFiber$1((fiber) => exitSucceed(f(get(fiber.context, this))));
	}
};
const cacheKeys = /*#__PURE__*/ new Set();
const ReferenceTypeId = "~effect/Context/Reference";
const TypeId$21 = "~effect/Context";
const FlattenAfterBaseHits = 8;
const makeImpl = (cacheRoot, base, overlay, depth, maxDepth) => {
	const self = Object.create(Proto$5);
	self.cacheRoot = cacheRoot ?? self;
	self.base = base;
	self.overlay = overlay;
	self.depth = depth;
	self._flat = void 0;
	self.baseHits = 0;
	self.maxDepth = maxDepth;
	return self;
};
const applyOverlays = (map, overlay) => {
	if (!overlay) return;
	applyOverlays(map, overlay.parent);
	map.set(overlay.key, overlay.value);
};
const flatten = (self) => {
	if (self._flat) return self._flat;
	if (!self.overlay) return self._flat = self.base;
	const map = new Map(self.base);
	applyOverlays(map, self.overlay);
	return self._flat = map;
};
const withFlat = (self, f) => {
	const map = new Map(self.mapUnsafe);
	f(map);
	return makeImpl(void 0, map, void 0, 0, self.maxDepth);
};
const notFound = /*#__PURE__*/ Symbol();
const lookup = (self, key) => {
	const impl = self;
	for (let overlay = impl.overlay; overlay; overlay = overlay.parent) if (overlay.key === key) return overlay.value;
	const value = impl.base.get(key);
	if (value === void 0 && !impl.base.has(key)) return notFound;
	if (impl.overlay && ++impl.baseHits >= impl.base.size && impl.baseHits >= FlattenAfterBaseHits) {
		impl.base = flatten(impl);
		impl.overlay = void 0;
		impl.depth = 0;
	}
	return value;
};
/**
* Creates a `Context` from an existing service map.
*
* **When to use**
*
* Use when constructing a low-level `Context` from a trusted map whose lifecycle
* you control.
*
* **Details**
*
* `options.maxDepth` sets how many `add` calls are kept as overlays before the
* context is rebased into a flat map (default `8`). Lower values make lookups
* cheaper, higher values make additions to a large base cheaper. Invalid
* values fall back to the default.
*
* Contexts derived with `add`, `pick`, `omit`, and `merge` keep the setting
* (`merge` from its first non-empty argument). `make`, `empty`, and `mergeAll`
* use the default. Layers are combined with `mergeAll`, so apply it to the
* final context rather than inside a layer.
*
* **Gotchas**
*
* Excessively large `maxDepth` values can cause stack overflow when overlays
* are materialized or rebased. Choose a modest value.
*
* The provided map is retained without copying and must not be mutated after
* construction. Prefer `empty`, `make`, `add`, or `merge` for normal Context
* construction.
*
* **Example** (Creating a context from a map)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a context from a Map (unsafe)
* const map = new Map([
*   ["Logger", { log: (_msg: string) => {} }]
* ])
*
* const context = Context.makeUnsafe(map)
* context.mapUnsafe.size // => 1
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe$5 = (mapUnsafe, options) => {
	const maxDepth = options?.maxDepth;
	return makeImpl(void 0, mapUnsafe, void 0, 0, Number.isInteger(maxDepth) && maxDepth >= 0 ? maxDepth : 8);
};
const Proto$5 = {
	get mapUnsafe() {
		return flatten(this);
	},
	...PipeInspectableProto,
	[TypeId$21]: { _Services: (_) => _ },
	toJSON() {
		return {
			_id: "Context",
			services: Array.from(this.mapUnsafe).map(([key, value]) => ({
				key,
				value
			}))
		};
	},
	[symbol](that) {
		if (!isContext(that)) return false;
		const self = this.mapUnsafe;
		const other = that.mapUnsafe;
		if (self.size !== other.size) return false;
		for (const [key, value] of self) if (!other.has(key) || !equals$1(value, other.get(key))) return false;
		return true;
	},
	[symbol$1]() {
		return number$1(this.mapUnsafe.size);
	}
};
/** @internal */
const hasSameCache = (self, that) => self.cacheRoot === that.cacheRoot;
/**
* Checks whether the provided argument is a `Context`.
*
* **When to use**
*
* Use to narrow an unknown value before passing it to APIs that require a
* `Context`.
*
* **Details**
*
* This checks the runtime `Context` marker and does not inspect which services
* the context contains.
*
* **Gotchas**
*
* This guard only proves that the value is a `Context`; it does not prove that
* any specific service is present.
*
* **Example** (Checking for contexts)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
* Context.isContext(Context.empty()) // => true
* ```
*
* @see {@link isKey} for checking service keys
* @see {@link isReference} for checking references with defaults
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isContext = (u) => hasProperty(u, TypeId$21);
/**
* Checks whether the provided argument is a `Reference`.
*
* **Example** (Checking for references)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const LoggerRef = Context.Reference("Logger", {
*   defaultValue: () => ({ log: (_msg: string) => {} })
* })
*
* Context.isReference(LoggerRef) // => true
* Context.isReference(Context.Service("Key")) // => false
* ```
*
* @stability stable
* @category guards
* @since 3.11.0
*/
const isReference = (u) => !!u[ReferenceTypeId];
/**
* Returns an empty `Context`.
*
* **Example** (Creating an empty context)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
* Context.empty().mapUnsafe.size // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const empty$1 = () => emptyContext;
const emptyContext = /*#__PURE__*/ makeUnsafe$5(/*#__PURE__*/ new Map());
/**
* Creates a new `Context` with a single service associated to the key.
*
* **Example** (Creating a context with one service)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
*
* const context = Context.make(Port, { PORT: 8080 })
*
* Context.get(context, Port).PORT // => 8080
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$13 = (key, service) => makeUnsafe$5(/* @__PURE__ */ new Map([[key.key, service]]));
/**
* Adds a service to a given `Context`.
*
* **When to use**
*
* Use when you need to store a known service value in a `Context`.
*
* **Details**
*
* If the context already contains the same service key, the new service
* replaces the previous one.
*
* **Example** (Adding a service to a context)
*
* ```ts import.meta.vitest
* import { Context, pipe } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const someContext = Context.make(Port, { PORT: 8080 })
*
* const context = pipe(
*   someContext,
*   Context.add(Timeout, { TIMEOUT: 5000 })
* )
*
* const values = [Context.get(context, Port).PORT, Context.get(context, Timeout).TIMEOUT]
* values // => [8080, 5000]
* ```
*
* @see {@link addOrOmit} for adding or removing a service from an `Option`
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const add = /*#__PURE__*/ dual(3, (self, key, service) => addUnsafe(self, key.key, service));
/**
* Adds a service by key to a given `Context` using a string key.
*
* @stability stable
* @category combining
* @since 4.0.0
*/
const addUnsafe = (self, key, service) => {
	const impl = self;
	const cacheRoot = cacheKeys.has(key) ? void 0 : impl.cacheRoot;
	if (impl.depth >= impl.maxDepth) {
		const map = new Map(impl._flat ?? impl.base);
		if (!impl._flat) applyOverlays(map, impl.overlay);
		map.set(key, service);
		return makeImpl(cacheRoot, map, void 0, 0, impl.maxDepth);
	}
	return makeImpl(cacheRoot, impl.base, {
		key,
		value: service,
		parent: impl.overlay
	}, impl.depth + 1, impl.maxDepth);
};
/** @internal */
const getOrUndefinedUnsafe = (self, key) => {
	const value = lookup(self, key);
	return value === notFound ? void 0 : value;
};
/**
* Gets a service from the context that corresponds to the given key.
*
* **When to use**
*
* Use when you need type-checked access to a service already included in the
* context type.
*
* **Example** (Getting a service from a context)
*
* ```ts import.meta.vitest
* import { Context, pipe } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const context = pipe(
*   Context.make(Port, { PORT: 8080 }),
*   Context.add(Timeout, { TIMEOUT: 5000 })
* )
*
* Context.get(context, Timeout).TIMEOUT // => 5000
* ```
*
* @see {@link getOption} for optional service access
* @see {@link getOrElse} for fallback values
*
* @stability stable
* @category getters
* @since 2.0.0
*/
const get = /* @__PURE__ */ dual(2, (self, service) => {
	const value = lookup(self, service.key);
	if (value === notFound) {
		if (isReference(service)) return getDefaultValue(service);
		throw serviceNotFoundError(service);
	}
	return value;
});
const defaultValueCacheKey = "~effect/Context/defaultValue";
const getDefaultValue = (ref) => {
	if (defaultValueCacheKey in ref) return ref[defaultValueCacheKey];
	return ref[defaultValueCacheKey] = ref.defaultValue();
};
const serviceNotFoundError = (service) => {
	const error = /* @__PURE__ */ new Error(`Service not found${service.key ? `: ${String(service.key)}` : ""}`);
	if (error.stack) {
		const lines = error.stack.split("\n");
		lines.splice(1, 3);
		error.stack = lines.join("\n");
	}
	return error;
};
/**
* Merges two `Context`s into one.
*
* **When to use**
*
* Use when you need to combine two contexts.
*
* **Details**
*
* When both contexts contain the same service key, the service from `that`
* overrides the service from `self`.
*
* **Example** (Merging two contexts)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* const Port = Context.Service<{ PORT: number }>("Port")
* const Timeout = Context.Service<{ TIMEOUT: number }>("Timeout")
*
* const firstContext = Context.make(Port, { PORT: 8080 })
* const secondContext = Context.make(Timeout, { TIMEOUT: 5000 })
*
* const context = Context.merge(firstContext, secondContext)
*
* const values = [Context.get(context, Port).PORT, Context.get(context, Timeout).TIMEOUT]
* values // => [8080, 5000]
* ```
*
* @see {@link mergeAll} for merging more than two contexts at once
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const merge = /*#__PURE__*/ dual(2, (self, that) => {
	if (self.mapUnsafe.size === 0) return that;
	if (that.mapUnsafe.size === 0) return self;
	return withFlat(self, (map) => that.mapUnsafe.forEach((value, key) => map.set(key, value)));
});
/**
* Creates a context key with a default value.
*
* **When to use**
*
* Use when you need to define a context key with a lazily computed default
* value.
*
* **Details**
*
* `Context.Reference` allows you to create a key that can hold a value. You
* can provide a default value for the service, which will automatically be used
* when the context is accessed, or override it with a custom implementation
* when needed. The default value is computed lazily and cached on the
* reference.
*
* **Example** (Creating references with default values)
*
* ```ts import.meta.vitest
* import { Context } from "effect"
*
* // Create a reference with a default value
* const messages: Array<string> = []
* const LoggerRef = Context.Reference("Logger", {
*   defaultValue: () => ({ log: (msg: string) => messages.push(`Default: ${msg}`) })
* })
*
* // The reference provides the default value when accessed from an empty context
* const context = Context.empty()
* const logger = Context.get(context, LoggerRef)
*
* // You can also override the default value
* const customContext = Context.make(LoggerRef, {
*   log: (msg: string) => messages.push(`Custom: ${msg}`)
* })
* const customLogger = Context.get(customContext, LoggerRef)
* logger.log("default")
* customLogger.log("message")
* messages // => ["Default: default", "Custom: message"]
* ```
*
* @see {@link Service} for required services without default values
*
* @stability stable
* @category services
* @since 3.11.0
*/
const Reference = Service;
// node_modules/effect/dist/Duration.js
const TypeId$20 = "~effect/Duration";
const bigint0$1 = /*#__PURE__*/ BigInt(0);
const bigint1$1 = /*#__PURE__*/ BigInt(1);
const bigint2 = /*#__PURE__*/ BigInt(2);
const bigint10$1 = /*#__PURE__*/ BigInt(10);
const bigint1e3 = /*#__PURE__*/ BigInt(1e3);
const bigint1e6 = /*#__PURE__*/ BigInt(1e6);
const bigint1e9 = /*#__PURE__*/ BigInt(1e9);
const bigintNanosPerMinute = /*#__PURE__*/ BigInt(6e10);
const bigintNanosPerHour = /*#__PURE__*/ BigInt(36e11);
const bigintNanosPerDay = /*#__PURE__*/ BigInt(864e11);
const bigintNanosPerWeek = /*#__PURE__*/ BigInt(6048e11);
const roundTiesAwayFromZero = (input) => BigInt(Math.sign(input) * Math.round(Math.abs(input)));
const roundMillisToNanos = (millis) => Number.isInteger(millis) ? BigInt(millis) * bigint1e6 : roundTiesAwayFromZero(millis * 1e6);
const fromNanosNumber = (nanos) => make$12(Number.isFinite(nanos) ? roundTiesAwayFromZero(nanos) : nanos);
const nanosToUnit = (nanos, divisor) => {
	const value = Number(nanos);
	if (Number.isFinite(value)) return value / divisor;
	const scale = BigInt(divisor);
	return Number(nanos / scale) + Number(nanos % scale) / divisor;
};
const parseNanos = (input, scale) => {
	const decimalIndex = input.indexOf(".");
	if (decimalIndex === -1) return BigInt(input) * scale;
	const isNegative = input[0] === "-";
	const fractional = input.slice(decimalIndex + 1);
	const fractionalScale = bigint10$1 ** BigInt(fractional.length);
	const scaled = (BigInt(input.slice(isNegative ? 1 : 0, decimalIndex)) * fractionalScale + BigInt(fractional)) * scale;
	return divideTiesAwayFromZero(isNegative ? -scaled : scaled, fractionalScale);
};
const divideTiesAwayFromZero = (numerator, denominator) => {
	const absolute = numerator < bigint0$1 ? -numerator : numerator;
	const rounded = absolute / denominator + (absolute % denominator * bigint2 >= denominator ? bigint1$1 : bigint0$1);
	return numerator < bigint0$1 ? -rounded : rounded;
};
const fromMillisString = (input, scale) => {
	const millis = Number(input) * scale;
	if (!Number.isFinite(millis)) return make$12(millis);
	const nanos = parseNanos(input, BigInt(scale) * bigint1e6);
	return make$12(Number.isInteger(millis) && BigInt(millis) * bigint1e6 === nanos ? millis : nanos);
};
const DURATION_REGEXP = /^(-?\d+(?:\.\d+)?)\s+(nanos?|micros?|millis?|seconds?|minutes?|hours?|days?|weeks?)$/;
/**
* Decodes a `Duration.Input` into a `Duration`.
*
* **When to use**
*
* Use when the input has already been validated or comes from a trusted source
* and throwing is acceptable for invalid duration syntax.
*
* **Gotchas**
*
* If the input is not a valid `Duration.Input`, it throws an error.
*
* **Example** (Decoding duration inputs)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.fromInputUnsafe(1000) // => Duration.millis(1000)
* Duration.fromInputUnsafe("5 seconds") // => Duration.seconds(5)
* Duration.fromInputUnsafe("Infinity") // => Duration.infinity
* Duration.fromInputUnsafe([2, 500_000_000]) // => Duration.nanos(2_500_000_000n)
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromInputUnsafe = (input) => {
	switch (typeof input) {
		case "number": return millis(input);
		case "bigint": return nanos(input);
		case "string": {
			if (input === "Infinity") return infinity;
			if (input === "-Infinity") return negativeInfinity;
			const match = DURATION_REGEXP.exec(input);
			if (!match) break;
			const [_, valueStr, unit] = match;
			if (unit === "nano" || unit === "nanos") return nanos(parseNanos(valueStr, bigint1$1));
			if (unit === "micro" || unit === "micros") return nanos(parseNanos(valueStr, bigint1e3));
			switch (unit) {
				case "milli":
				case "millis": return fromMillisString(valueStr, 1);
				case "second":
				case "seconds": return fromMillisString(valueStr, 1e3);
				case "minute":
				case "minutes": return fromMillisString(valueStr, 6e4);
				case "hour":
				case "hours": return fromMillisString(valueStr, 36e5);
				case "day":
				case "days": return fromMillisString(valueStr, 864e5);
				case "week":
				case "weeks": return fromMillisString(valueStr, 6048e5);
			}
			break;
		}
		case "object": {
			if (input === null) break;
			if (TypeId$20 in input) return input;
			if (Array.isArray(input)) {
				if (input.length !== 2 || !input.every(isNumber)) return invalid$1(input);
				if (Number.isNaN(input[0]) || Number.isNaN(input[1])) return zero$1;
				if (input[0] === -Infinity || input[1] === -Infinity) return negativeInfinity;
				if (input[0] === Infinity || input[1] === Infinity) return infinity;
				const nanos = input[0] * 1e9 + input[1];
				return Number.isFinite(nanos) && Number.isInteger(input[0]) && Number.isInteger(input[1]) ? make$12(BigInt(input[0]) * bigint1e9 + BigInt(input[1])) : fromNanosNumber(nanos);
			}
			const obj = input;
			let millis = 0;
			if (obj.weeks) millis += obj.weeks * 6048e5;
			if (obj.days) millis += obj.days * 864e5;
			if (obj.hours) millis += obj.hours * 36e5;
			if (obj.minutes) millis += obj.minutes * 6e4;
			if (obj.seconds) millis += obj.seconds * 1e3;
			if (obj.milliseconds) millis += obj.milliseconds;
			if (!obj.microseconds && !obj.nanoseconds) {
				if (!Number.isFinite(millis)) return make$12(millis);
				const exact = sumObjectNanos(obj);
				return exact === void 0 || BigInt(millis) * bigint1e6 === exact ? make$12(millis) : make$12(exact);
			}
			const nanos = millis * 1e6 + (obj.microseconds ?? 0) * 1e3 + (obj.nanoseconds ?? 0);
			const exact = Number.isFinite(nanos) ? sumObjectNanos(obj) : void 0;
			return exact === void 0 ? fromNanosNumber(nanos) : make$12(exact);
		}
	}
	return invalid$1(input);
};
const isIntegerOrUnset = (n) => !n || Number.isInteger(n);
const sumObjectNanos = (obj) => {
	if (!isIntegerOrUnset(obj.weeks) || !isIntegerOrUnset(obj.days) || !isIntegerOrUnset(obj.hours) || !isIntegerOrUnset(obj.minutes) || !isIntegerOrUnset(obj.seconds) || !isIntegerOrUnset(obj.milliseconds) || !isIntegerOrUnset(obj.microseconds) || !isIntegerOrUnset(obj.nanoseconds)) return void 0;
	let nanos = bigint0$1;
	if (obj.weeks) nanos += BigInt(obj.weeks) * bigintNanosPerWeek;
	if (obj.days) nanos += BigInt(obj.days) * bigintNanosPerDay;
	if (obj.hours) nanos += BigInt(obj.hours) * bigintNanosPerHour;
	if (obj.minutes) nanos += BigInt(obj.minutes) * bigintNanosPerMinute;
	if (obj.seconds) nanos += BigInt(obj.seconds) * bigint1e9;
	if (obj.milliseconds) nanos += BigInt(obj.milliseconds) * bigint1e6;
	if (obj.microseconds) nanos += BigInt(obj.microseconds) * bigint1e3;
	if (obj.nanoseconds) nanos += BigInt(obj.nanoseconds);
	return nanos;
};
const invalid$1 = (input) => {
	throw new Error(`Invalid Input: ${input}`);
};
const zeroDurationValue = {
	_tag: "Millis",
	millis: 0
};
const infinityDurationValue = { _tag: "Infinity" };
const negativeInfinityDurationValue = { _tag: "NegativeInfinity" };
const DurationProto = {
	[TypeId$20]: TypeId$20,
	[symbol$1]() {
		switch (this.value._tag) {
			case "Millis": return hash(roundMillisToNanos(this.value.millis));
			case "Nanos": return hash(this.value.nanos);
			default: return structure(this.value);
		}
	},
	[symbol](that) {
		return isDuration(that) && equals(this, that);
	},
	toString() {
		switch (this.value._tag) {
			case "Infinity": return "Infinity";
			case "NegativeInfinity": return "-Infinity";
			case "Nanos": return `${this.value.nanos} nanos`;
			case "Millis": return `${this.value.millis} millis`;
		}
	},
	toJSON() {
		switch (this.value._tag) {
			case "Millis": return {
				_id: "Duration",
				_tag: "Millis",
				millis: this.value.millis
			};
			case "Nanos": return {
				_id: "Duration",
				_tag: "Nanos",
				nanos: String(this.value.nanos)
			};
			case "Infinity": return {
				_id: "Duration",
				_tag: "Infinity"
			};
			case "NegativeInfinity": return {
				_id: "Duration",
				_tag: "NegativeInfinity"
			};
		}
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const make$12 = (input) => {
	const duration = Object.create(DurationProto);
	if (typeof input === "number") {
		if (isNaN(input) || input === 0 || Object.is(input, -0)) duration.value = zeroDurationValue;
		else if (!Number.isFinite(input)) duration.value = input > 0 ? infinityDurationValue : negativeInfinityDurationValue;
		else if (!Number.isInteger(input)) duration.value = {
			_tag: "Nanos",
			nanos: roundMillisToNanos(input)
		};
		else duration.value = {
			_tag: "Millis",
			millis: input
		};
	} else if (input === bigint0$1) duration.value = zeroDurationValue;
	else duration.value = {
		_tag: "Nanos",
		nanos: input
	};
	return duration;
};
/**
* Checks whether a value is a Duration.
*
* **Example** (Checking for durations)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.isDuration(Duration.seconds(1)) // => true
* Duration.isDuration(1000) // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isDuration = (u) => hasProperty(u, TypeId$20);
/**
* A Duration representing zero time.
*
* **Example** (Referencing the zero duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.zero) // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const zero$1 = /*#__PURE__*/ make$12(0);
/**
* A Duration representing infinite time.
*
* **Example** (Referencing infinite duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.infinity) // => Infinity
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const infinity = /*#__PURE__*/ make$12(Infinity);
/**
* A Duration representing negative infinite time.
*
* **Example** (Referencing negative infinite duration)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.negativeInfinity) // => -Infinity
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const negativeInfinity = /*#__PURE__*/ make$12(-Infinity);
/**
* Creates a Duration from nanoseconds.
*
* **Example** (Creating durations from nanoseconds)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.nanos(500_000_000n) // => Duration.nanos(500_000_000n)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const nanos = (nanos) => make$12(nanos);
/**
* Creates a Duration from milliseconds.
*
* **Example** (Creating durations from milliseconds)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.millis(1000)) // => 1000
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const millis = (millis) => make$12(millis);
/**
* Converts a Duration to milliseconds.
*
* **Example** (Converting durations to milliseconds)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toMillis(Duration.seconds(5)) // => 5000
* Duration.toMillis(Duration.minutes(2)) // => 120_000
* ```
*
* @stability stable
* @category getters
* @since 2.0.0
*/
const toMillis = (self) => match$1(fromInputUnsafe(self), {
	onMillis: identity,
	onNanos: (nanos) => nanosToUnit(nanos, 1e6),
	onInfinity: () => Infinity,
	onNegativeInfinity: () => -Infinity
});
/**
* Gets the duration in nanoseconds as a bigint.
*
* **When to use**
*
* Use when the duration is known to be finite and you need the nanosecond value
* as a `bigint`.
*
* **Details**
*
* Millisecond-backed fractional durations are rounded to the nearest
* nanosecond, with ties away from zero.
*
* **Gotchas**
*
* If the duration is infinite, it throws an error.
*
* **Example** (Reading nanoseconds unsafely)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.toNanosUnsafe(Duration.seconds(2)) // => 2_000_000_000n
*
* // Duration.toNanosUnsafe(Duration.infinity)
* // throws Error: "Cannot convert infinite duration to nanos"
* ```
*
* @stability stable
* @category getters
* @since 4.0.0
*/
const toNanosUnsafe = (input) => {
	const self = fromInputUnsafe(input);
	switch (self.value._tag) {
		case "Infinity":
		case "NegativeInfinity": throw new Error("Cannot convert infinite duration to nanos");
		case "Nanos": return self.value.nanos;
		case "Millis": return roundMillisToNanos(self.value.millis);
	}
};
/**
* Pattern matches on the representation of a `Duration`.
*
* **Details**
*
* Provide handlers for millisecond-backed values, nanosecond-backed values,
* and positive infinity. Use `onNegativeInfinity` to handle negative infinity
* separately; otherwise negative infinity is handled by `onInfinity`.
*
* **Example** (Pattern matching on duration representations)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.match(Duration.seconds(5), {
*   onMillis: (millis) => `${millis} milliseconds`,
*   onNanos: (nanos) => `${nanos} nanoseconds`,
*   onInfinity: () => "infinite"
* }) // => "5000 milliseconds"
* ```
*
* @stability stable
* @category pattern matching
* @since 2.0.0
*/
const match$1 = /*#__PURE__*/ dual(2, (self, options) => {
	switch (self.value._tag) {
		case "Millis": return options.onMillis(self.value.millis);
		case "Nanos": return options.onNanos(self.value.nanos);
		case "Infinity": return options.onInfinity();
		case "NegativeInfinity": return (options.onNegativeInfinity ?? options.onInfinity)();
	}
});
/**
* Pattern matches on two `Duration`s, providing handlers that receive both values.
*
* **Example** (Pattern matching on duration pairs)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.matchPair(Duration.seconds(3), Duration.seconds(2), {
*   onMillis: (a, b) => a + b,
*   onNanos: (a, b) => Number(a + b),
*   onInfinity: () => Infinity
* }) // => 5000
* ```
*
* @stability stable
* @category pattern matching
* @since 4.0.0
*/
const matchPair = /*#__PURE__*/ dual(3, (self, that, options) => {
	if (self.value._tag === "Infinity" || self.value._tag === "NegativeInfinity" || that.value._tag === "Infinity" || that.value._tag === "NegativeInfinity") return options.onInfinity(self, that);
	if (self.value._tag === "Millis") return that.value._tag === "Millis" ? options.onMillis(self.value.millis, that.value.millis) : options.onNanos(toNanosUnsafe(self), that.value.nanos);
	else return options.onNanos(self.value.nanos, toNanosUnsafe(that));
});
/**
* Provides an `Equivalence` instance for comparing `Duration` values.
*
* **Example** (Comparing durations for equivalence)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.Equivalence(Duration.seconds(5), Duration.millis(5000)) // => true
* ```
*
* @stability stable
* @category instances
* @since 2.0.0
*/
const Equivalence$3 = (self, that) => matchPair(self, that, {
	onMillis: (self, that) => self === that,
	onNanos: (self, that) => self === that,
	onInfinity: (self, that) => self.value._tag === that.value._tag
});
/**
* Checks whether two Durations are equal.
*
* **Example** (Checking duration equality)
*
* ```ts import.meta.vitest
* import { Duration } from "effect"
*
* Duration.equals(Duration.seconds(5), Duration.millis(5000)) // => true
* ```
*
* @stability stable
* @category predicates
* @since 2.0.0
*/
const equals = /*#__PURE__*/ dual(2, (self, that) => Equivalence$3(self, that));
// node_modules/effect/dist/internal/array.js
/**
* @since 2.0.0
*/
/** @internal */
const isArrayNonEmpty$1 = (self) => self.length > 0;
// node_modules/effect/dist/internal/count.js
/**
* Normalizes a collection count to a non-negative integer. `NaN` and
* non-positive values become `0`; positive infinity is preserved.
*
* @internal
*/
const normalize$1 = (n) => n > 0 ? Math.floor(n) : 0;
// node_modules/effect/dist/Result.js
/**
* Creates a `Result` holding a `Success` value.
*
* **Details**
*
* - Use when you have a value and want to lift it into the `Result` type
* - The error type `E` defaults to `never`
*
* **Example** (Wrapping a value)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* Result.succeed(42) // => Result.succeed(42)
* ```
*
* @see {@link fail} to create a Failure
* @see {@link void_ void} for a pre-built `Success<void>`
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const succeed$4 = succeed$5;
/**
* Creates a `Result` holding a `Failure` value.
*
* **When to use**
*
* Use to represent a failed `Result` with a typed failure value.
*
* **Details**
*
* - The success type `A` defaults to `never`
*
* **Example** (Creating a failure)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* Result.fail("Something went wrong") // => Result.fail("Something went wrong")
* ```
*
* @see {@link succeed} to create a Success
* @see {@link mapError} to transform the error
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fail$4 = fail$5;
/**
* Checks whether a `Result` is a `Failure`.
*
* **When to use**
*
* Use to narrow a known `Result` to the `Failure` variant.
*
* **Details**
*
* - Acts as a TypeScript type guard, narrowing to `Failure<A, E>`
* - After narrowing, you can access `.failure` to read the error value
*
* **Example** (Narrowing to failure)
*
* ```ts import.meta.vitest
* import { Result } from "effect"
*
* const result = Result.fail("oops")
*
* if (Result.isFailure(result)) {
*   result.failure // => "oops"
* }
* ```
*
* @see {@link isSuccess} for the opposite check
* @see {@link isResult} to check if a value is any Result
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isFailure = isFailure$1;
// node_modules/effect/dist/Array.js
/**
* Exposes the global array constructor.
*
* **When to use**
*
* Use to access native JavaScript array constructor methods such as `isArray`
* or `from` from the Effect module namespace.
*
* **Example** (Accessing the Array constructor)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.Array === globalThis.Array // => true
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Array$1 = globalThis.Array;
/**
* Converts an `Iterable` to an `Array`.
*
* **When to use**
*
* Use to convert any `Iterable` (Set, Generator, etc.) into an array.
*
* **Details**
*
* If the input is already an array, this returns it by reference without
* copying. Otherwise, it creates a new array from the iterable. Use `copy` if
* you need a fresh array even when the input is already an array.
*
* **Example** (Converting a Set to an array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.fromIterable(new Set([1, 2, 3])) // => [1, 2, 3]
* ```
*
* @see {@link ensure} — wrap a single value or return an existing array
* @see {@link copy} — create a shallow copy of an array
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromIterable$1 = (collection) => Array$1.isArray(collection) ? collection : Array$1.from(collection);
/**
* Adds a single element to the end of an iterable, returning a `NonEmptyArray`.
*
* **When to use**
*
* Use when you need to guarantee a non-empty result after adding a required
* trailing value.
*
* **Example** (Appending an element)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.append([1, 2, 3], 4) // => [1, 2, 3, 4]
* ```
*
* @see {@link prepend} — add to the front
* @see {@link appendAll} — append multiple elements
*
* @stability stable
* @category combining
* @since 2.0.0
*/
const append$1 = /*#__PURE__*/ dual(2, (self, last) => [...self, last]);
Array$1.isArray;
/**
* Checks whether a mutable `Array` is non-empty, narrowing the type to
* `NonEmptyArray`.
*
* **When to use**
*
* Use when you need the narrowed value to remain a mutable `Array` after proving
* it has at least one element.
*
* **Example** (Checking for a non-empty array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.isArrayNonEmpty([]) // => false
* Array.isArrayNonEmpty([1, 2, 3]) // => true
* ```
*
* @see {@link isReadonlyArrayNonEmpty} — readonly variant
* @see {@link isArrayEmpty} — opposite check
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isArrayNonEmpty = isArrayNonEmpty$1;
/**
* Checks whether a `ReadonlyArray` is non-empty, narrowing the type to
* `NonEmptyReadonlyArray`.
*
* **When to use**
*
* Use when you need to prove a readonly array has at least one element without
* requiring mutable array methods afterward.
*
* **Example** (Checking for a non-empty readonly array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.isReadonlyArrayNonEmpty([]) // => false
* Array.isReadonlyArrayNonEmpty([1, 2, 3]) // => true
* ```
*
* @see {@link isArrayNonEmpty} — mutable variant
* @see {@link isReadonlyArrayEmpty} — opposite check
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isReadonlyArrayNonEmpty = isArrayNonEmpty$1;
/**
* Creates an empty array.
*
* **When to use**
*
* Use to create a typed empty array without allocating placeholder elements.
*
* **Example** (Creating an empty array)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.empty<number>() // => []
* ```
*
* @see {@link of} — create a single-element array
* @see {@link make} — create from multiple values
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const empty = () => [];
/**
* Transforms each element using a function, returning a new array.
*
* **When to use**
*
* Use to transform each element independently while preserving the array shape.
*
* **Details**
*
* The function receives `(element, index)`. The return type preserves
* `NonEmptyArray`.
*
* **Example** (Doubling values)
*
* ```ts import.meta.vitest
* import { Array } from "effect"
*
* Array.map([1, 2, 3], (x) => x * 2) // => [2, 4, 6]
* ```
*
* @see {@link flatMap} — map and flatten
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map$3 = /*#__PURE__*/ dual(2, (self, f) => self.map(f));
// node_modules/effect/dist/Scheduler.js
/**
* Controls how runnable Effect fiber tasks are dispatched.
*
* A scheduler decides how tasks are queued, when queued tasks run, and when a
* fiber should pause so other work can continue. This module includes the
* scheduler service reference, the default `MixedScheduler`, dispatcher types
* for queued tasks, and references for tuning or disabling automatic scheduler
* yields.
*
* @stability stable
* @since 2.0.0
*/
/**
* Context reference for the scheduler used by the Effect runtime.
*
* **When to use**
*
* Use when you need to replace scheduling behavior globally in tests or runtime
* setup, such as forcing deterministic task dispatch.
*
* **Details**
*
* The default value creates a `MixedScheduler`. Provide this service to
* customize execution mode, task dispatching, or yield behavior.
*
* @stability stable
* @category services
* @since 2.0.0
*/
const Scheduler = /*#__PURE__*/ Reference("effect/Scheduler", {
	fiberCached: true,
	defaultValue: () => new MixedScheduler()
});
const setMicrotask = (f) => {
	let cancelled = false;
	Promise.resolve().then(() => {
		if (!cancelled) f();
	});
	return () => {
		cancelled = true;
	};
};
const setTimer = "setImmediate" in globalThis ? (f) => {
	const timer = globalThis.setImmediate(f);
	return () => globalThis.clearImmediate(timer);
} : (f) => {
	const timer = setTimeout(f, 0);
	return () => clearTimeout(timer);
};
const setImmediate = (f) => {
	try {
		return setTimer(f);
	} catch {
		return setMicrotask(f);
	}
};
var PriorityBuckets = class {
	buckets = [];
	scheduleTask(task, priority) {
		const buckets = this.buckets;
		const len = buckets.length;
		let bucket;
		let index = 0;
		for (; index < len; index++) {
			if (buckets[index][0] > priority) break;
			bucket = buckets[index];
		}
		if (bucket && bucket[0] === priority) bucket[1].push(task);
		else if (index === len) buckets.push([priority, [task]]);
		else buckets.splice(index, 0, [priority, [task]]);
	}
	drain() {
		const buckets = this.buckets;
		this.buckets = [];
		return buckets;
	}
};
/**
* Provides a scheduler implementation that batches queued tasks and dispatches them by
* priority.
*
* **When to use**
*
* Use when you need the default runtime scheduler directly, including a
* scheduler that batches queued work by priority and preserves FIFO order within
* each priority.
*
* **Details**
*
* `MixedScheduler` supports synchronous and asynchronous execution modes, uses
* operation counts to decide when fibers should yield, and is the default
* scheduler implementation.
*
* **Gotchas**
*
* On Cloudflare Workers, request-owned scheduler timers can be discarded,
* leaving shared fibers suspended. Use `new MixedScheduler("sync")` for the
* whole application; see `HttpRouter.toWebHandler` for a recipe.
* Sync mode does not yield to the event loop: CPU-heavy work delays other
* requests, and I/O-polling loops can exhaust the CPU limit. It does not fix
* request-owned timers (such as `Effect.sleep`) or request-bound async contexts.
*
* @stability stable
* @category models
* @since 2.0.0
*/
var MixedScheduler = class {
	executionMode;
	setImmediate;
	constructor(executionMode = "async", setImmediateFn) {
		this.executionMode = executionMode;
		this.setImmediate = setImmediateFn ?? (executionMode === "sync" ? setMicrotask : setImmediate);
	}
	/**
	* Returns whether the fiber has reached its operation budget and should yield.
	*
	* **When to use**
	*
	* Use to decide whether a fiber should yield after consuming its current
	* operation budget.
	*
	* @since 2.0.0
	*/
	shouldYield(fiber) {
		return fiber.currentOpCount >= fiber.cache.maxOpsBeforeYield;
	}
	/**
	* Creates a dispatcher that schedules work through this scheduler.
	*
	* **When to use**
	*
	* Use when you need a standalone dispatcher from a scheduler instance, for
	* example in tests that enqueue tasks and then flush them deterministically.
	*
	* @since 4.0.0
	*/
	makeDispatcher() {
		return new MixedSchedulerDispatcher(this.setImmediate);
	}
};
var MixedSchedulerDispatcher = class {
	tasks = /*#__PURE__*/ new PriorityBuckets();
	running = void 0;
	setImmediate;
	constructor(setImmediateFn = setImmediate) {
		this.setImmediate = setImmediateFn;
	}
	/**
	* @since 2.0.0
	*/
	scheduleTask(task, priority) {
		this.tasks.scheduleTask(task, priority);
		if (this.running === void 0) this.running = this.setImmediate(this.afterScheduled);
	}
	/**
	* @since 2.0.0
	*/
	afterScheduled = () => {
		this.running = void 0;
		this.runTasks();
	};
	/**
	* @since 2.0.0
	*/
	runTasks() {
		const buckets = this.tasks.drain();
		for (let i = 0; i < buckets.length; i++) {
			const toRun = buckets[i][1];
			for (let j = 0; j < toRun.length; j++) toRun[j]();
		}
	}
	/**
	* @since 2.0.0
	*/
	flush() {
		while (this.tasks.buckets.length > 0) {
			if (this.running !== void 0) {
				this.running();
				this.running = void 0;
			}
			this.runTasks();
		}
	}
};
/**
* Context reference that controls the maximum number of operations a fiber
* can perform before yielding control back to the scheduler.
*
* **When to use**
*
* Use to tune scheduler fairness for CPU-bound fibers by changing the scheduler
* operation budget that triggers a yield.
*
* **Details**
*
* The default value is `2048` operations, which balances performance and
* fairness by helping prevent long-running fibers from monopolizing the
* execution thread.
*
* @see {@link PreventSchedulerYield} for bypassing scheduler yield checks entirely rather than tuning the operation budget
*
* @stability stable
* @category services
* @since 4.0.0
*/
const MaxOpsBeforeYield = /*#__PURE__*/ Reference("effect/Scheduler/MaxOpsBeforeYield", {
	fiberCached: true,
	defaultValue: () => 2048
});
/**
* Context reference that controls whether the runtime should bypass scheduler
* yield checks. When set to `true`, the fiber run loop won't call
* `Scheduler.shouldYield`.
*
* **When to use**
*
* Use to bypass scheduler yield checks for controlled runtime workloads where
* cooperative yielding should be disabled.
*
* **Gotchas**
*
* Setting this reference to `true` can let long-running fibers monopolize the
* JavaScript thread.
*
* @see {@link MaxOpsBeforeYield} for tuning yield frequency without disabling yield checks
* @see {@link Scheduler} for providing custom scheduler yield behavior
*
* @stability stable
* @category services
* @since 4.0.0
*/
const PreventSchedulerYield = /*#__PURE__*/ Reference("effect/Scheduler/PreventSchedulerYield", {
	fiberCached: true,
	defaultValue: () => false
});
// node_modules/effect/dist/Tracer.js
/**
* Defines the string key for the parent-span context service.
*
* **When to use**
*
* Use when you need the raw context key for parent span lookup in lower-level
* tracing code.
*
* **Example** (Reading the parent span key)
*
* ```ts import.meta.vitest
* import { Tracer } from "effect"
*
* // The key used to identify parent spans in the context
* Tracer.ParentSpanKey // => "effect/Tracer/ParentSpan"
* ```
*
* @stability stable
* @category constants
* @since 4.0.0
*/
const ParentSpanKey = "effect/Tracer/ParentSpan";
/**
* Defines the string key for the active tracer context reference.
*
* **When to use**
*
* Use when you need the raw context key for active tracer lookup in lower-level
* tracing code.
*
* @stability stable
* @category constants
* @since 4.0.0
*/
const TracerKey = "effect/Tracer";
// node_modules/effect/dist/internal/metric.js
/** @internal */
const FiberRuntimeMetricsKey = "effect/Metric/FiberRuntimeMetrics";
// node_modules/effect/dist/internal/references.js
/** @internal */
const CurrentStackFrame = /*#__PURE__*/ Reference("effect/References/CurrentStackFrame", {
	fiberCached: true,
	defaultValue: constUndefined
});
/** @internal */
const TracerEnabled = /*#__PURE__*/ Reference("effect/References/TracerEnabled", {
	fiberCached: true,
	defaultValue: constTrue
});
/** @internal */
const CurrentLogAnnotations = /*#__PURE__*/ Reference("effect/References/CurrentLogAnnotations", { defaultValue: () => ({}) });
/** @internal */
const CurrentLogLevel = /*#__PURE__*/ Reference("effect/References/CurrentLogLevel", {
	fiberCached: true,
	defaultValue: () => "Info"
});
/** @internal */
const MinimumLogLevel = /*#__PURE__*/ Reference("effect/References/MinimumLogLevel", {
	fiberCached: true,
	defaultValue: () => "Info"
});
/** @internal */
const UnhandledLogLevel = /*#__PURE__*/ Reference("effect/References/UnhandledLogLevel", { defaultValue: () => "Error" });
/** @internal */
const CurrentLogSpans = /*#__PURE__*/ Reference("effect/References/CurrentLogSpans", { defaultValue: () => [] });
// node_modules/effect/dist/internal/effect.js
/** @internal */
var Interrupt = class extends ReasonBase {
	constructor(fiberId, annotations = constEmptyAnnotations) {
		super("Interrupt", annotations, "Interrupted");
		this.fiberId = fiberId;
	}
	toString() {
		return `Interrupt(${this.fiberId})`;
	}
	toJSON() {
		return {
			_tag: "Interrupt",
			fiberId: this.fiberId
		};
	}
	[symbol](that) {
		return isInterruptReason(that) && this.fiberId === that.fiberId && this.annotations === that.annotations;
	}
	[symbol$1]() {
		return string$1(`${this._tag}:${this.fiberId}`);
	}
};
/** @internal */
const causeInterrupt = (fiberId) => new CauseImpl([new Interrupt(fiberId)]);
/** @internal */
const hasFails = (self) => self.reasons.some(isFailReason$1);
/** @internal */
const findError = (self) => {
	for (let i = 0; i < self.reasons.length; i++) {
		const reason = self.reasons[i];
		if (reason._tag === "Fail") return succeed$4(reason.error);
	}
	return fail$4(self);
};
/** @internal */
const hasDies = (self) => self.reasons.some(isDieReason);
/**
* Defects take precedence over typed failures when causes are combined: if
* the cause contains a `Die`, every `Fail` becomes a `Die`. `Done` signals
* stay typed so pulls still complete.
*
* @internal
*/
const causeElectDefects = (self) => hasDies(self) && hasFails(self) ? causeFromReasons(self.reasons.map((reason) => reason._tag === "Fail" && !isDone$1(reason.error) ? new Die(reason.error, reason.annotations) : reason)) : self;
/**
* Selects the first typed error unless a defect is present, in which case
* every `Fail` is elected to `Die`. Interrupts do not block typed recovery.
*
* @internal
*/
const findRecoverableError = (self) => hasDies(self) ? fail$4(causeElectDefects(self)) : findError(self);
/** @internal */
const hasInterrupts = (self) => self.reasons.some(isInterruptReason);
/** @internal */
const causeFilterInterruptors = (self) => {
	let interruptors;
	for (let i = 0; i < self.reasons.length; i++) {
		const f = self.reasons[i];
		if (f._tag !== "Interrupt") continue;
		interruptors ??= /* @__PURE__ */ new Set();
		if (f.fiberId !== void 0) interruptors.add(f.fiberId);
	}
	return interruptors ? succeed$4(interruptors) : fail$4(self);
};
/** @internal */
const causeMap = /*#__PURE__*/ dual(2, (self, f) => {
	let hasFail = false;
	const failures = self.reasons.map((failure) => {
		if (isFailReason$1(failure)) {
			hasFail = true;
			return new Fail(f(failure.error), failure.annotations);
		}
		return failure;
	});
	return hasFail ? causeFromReasons(failures) : self;
});
/** @internal */
const causeSquash = (self) => {
	let die;
	for (const reason of self.reasons) {
		if (reason._tag === "Fail") return reason.error;
		if (reason._tag === "Die") die ??= reason;
	}
	if (die !== void 0) return die.defect;
	if (self.reasons.length > 0) return new globalThis.Error("All fibers interrupted without error");
	return new globalThis.Error("Empty cause");
};
/** @internal */
const causePrettyErrors = (self, options) => {
	const errors = [];
	const interrupts = [];
	if (self.reasons.length === 0) return errors;
	const prevStackLimit = getStackTraceLimit();
	if (prevStackLimit !== 0) setStackTraceLimit(1);
	try {
		for (const failure of self.reasons) {
			if (failure._tag === "Interrupt") {
				interrupts.push(failure);
				continue;
			}
			errors.push(causePrettyError(failure._tag === "Die" ? failure.defect : failure.error, failure.annotations, options));
		}
		if (errors.length === 0) {
			const cause = /* @__PURE__ */ new Error("The fiber was interrupted by:");
			cause.name = "InterruptCause";
			cause.stack = interruptCauseStack(cause, interrupts);
			const error = new globalThis.Error("All fibers interrupted without error", { cause });
			error.name = "InterruptError";
			error.stack = `${error.name}: ${error.message}`;
			errors.push(causePrettyError(error, interrupts[0].annotations, options));
		}
	} finally {
		if (prevStackLimit !== 0) setStackTraceLimit(prevStackLimit);
	}
	return errors;
};
/** @internal */
const causePrettyError = (original, annotations, options, seen = /* @__PURE__ */ new Set()) => {
	const kind = typeof original;
	let error;
	if (original && kind === "object") {
		seen.add(original);
		const cause = original.cause;
		error = new globalThis.Error(causePrettyMessage(original), { cause: cause !== void 0 && !seen.has(cause) ? causePrettyError(cause, void 0, void 0, seen) : void 0 });
		if (typeof original.name === "string") error.name = original.name;
		if (typeof original.stack === "string") error.stack = cleanErrorStack(original.stack, error, annotations);
		else {
			const stack = `${error.name}: ${error.message}`;
			error.stack = annotations ? addStackAnnotations(stack, annotations) : stack;
		}
		if (options?.includeCauseInStack) error.stack = renderPrettyError(error);
		for (const key of Object.keys(original)) if (!(key in error)) error[key] = original[key];
	} else {
		error = new globalThis.Error(!original ? `Unknown error: ${original}` : kind === "string" ? original : formatJson(original));
		const stack = `${error.name}: ${error.message}`;
		error.stack = annotations ? addStackAnnotations(stack, annotations) : stack;
	}
	return error;
};
const causePrettyMessage = (u) => {
	if (typeof u.message === "string") return u.message;
	else if (typeof u.toString === "function" && u.toString !== Object.prototype.toString && u.toString !== Array.prototype.toString) try {
		return u.toString();
	} catch {}
	return formatJson(u);
};
const locationRegExp = /\((.*)\)/g;
const cleanErrorStack = (stack, error, annotations) => {
	const message = `${error.name}: ${error.message}`;
	const lines = (stack.startsWith(message) ? stack.slice(message.length) : stack).split("\n");
	const out = [message];
	for (let i = 1; i < lines.length; i++) {
		if (/Generator\.next|~effect\/(?:Effect|Utils)/.test(lines[i])) break;
		out.push(lines[i]);
	}
	return annotations ? addStackAnnotations(out.join("\n"), annotations) : out.join("\n");
};
const addStackAnnotations = (stack, annotations) => {
	const frame = annotations?.get(StackTraceKey.key);
	if (frame) stack = `${stack}\n${currentStackTrace(frame)}`;
	return stack;
};
const interruptCauseStack = (error, interrupts) => {
	const out = [`${error.name}: ${error.message}`];
	for (const current of interrupts) {
		const fiberId = current.fiberId !== void 0 ? `#${current.fiberId}` : "unknown";
		const frame = current.annotations.get(InterruptorStackTrace.key);
		out.push(`    at fiber (${fiberId})`);
		if (frame) out.push(currentStackTrace(frame));
	}
	return out.join("\n");
};
const currentStackTrace = (frame) => {
	const out = [];
	let current = frame;
	let i = 0;
	while (current && i < 10) {
		const stack = current.stack();
		if (stack) {
			const locationMatchAll = stack.matchAll(locationRegExp);
			let match = false;
			for (const [, location] of locationMatchAll) {
				match = true;
				out.push(`    at ${current.name} (${location})`);
			}
			if (!match) out.push(`    at ${current.name} (${stack.replace(/^at /, "")})`);
		} else out.push(`    at ${current.name}`);
		current = current.parent;
		i++;
	}
	return out.join("\n");
};
/** @internal */
const causePretty = (cause) => causePrettyErrors(cause).map(renderPrettyError).join("\n");
const renderPrettyError = (e) => e.cause ? `${e.stack} {\n${renderErrorCause(e.cause, "  ")}\n}` : e.stack;
const renderErrorCause = (cause, prefix) => {
	const lines = cause.stack.split("\n");
	let stack = `${prefix}[cause]: ${lines[0]}`;
	for (let i = 1, len = lines.length; i < len; i++) stack += `\n${prefix}${lines[i]}`;
	if (cause.cause) stack += ` {\n${renderErrorCause(cause.cause, `${prefix}  `)}\n${prefix}}`;
	return stack;
};
/** @internal */
const FiberTypeId = "~effect/Fiber";
const fiberVariance = {
	_A: identity,
	_E: identity
};
const fiberIdStore = { id: 0 };
const AsyncResource = /*#__PURE__*/ (() => {
	try {
		return globalThis.process?.getBuiltinModule?.("node:async_hooks")?.AsyncResource;
	} catch {
		return;
	}
})();
const captureAsyncContext = () => AsyncResource === void 0 ? void 0 : new AsyncResource("effect/Fiber");
/** @internal */
const getCurrentFiber = () => globalThis[currentFiberTypeId];
/** @internal */
var FiberImpl = class {
	constructor(context, interruptible = true) {
		this.setContext(context);
		this.id = ++fiberIdStore.id;
		this.currentOpCount = 0;
		this.interruptible = interruptible;
		this._stack = [];
		this._observers = void 0;
		this._exit = void 0;
		this._children = void 0;
		this._interruptedCause = void 0;
		this._yielded = void 0;
		this._running = false;
		this._deferredInterrupt = false;
		this._parent = void 0;
		this._asyncContext = void 0;
		this.cache.runtimeMetrics?.recordFiberStart(this.context);
	}
	get [FiberTypeId]() {
		return fiberVariance;
	}
	get currentDispatcher() {
		return this._dispatcher ??= this.cache.scheduler.makeDispatcher();
	}
	getRef(ref) {
		return get(this.context, ref);
	}
	addObserver(cb) {
		if (this._exit) {
			cb(this._exit);
			return constVoid;
		}
		if (this._observers === void 0) this._observers = [cb];
		else this._observers.push(cb);
		return () => this.removeObserver(cb);
	}
	removeObserver(cb) {
		if (this._exit || this._observers === void 0) return;
		const index = this._observers.indexOf(cb);
		if (index >= 0) this._observers.splice(index, 1);
	}
	interruptUnsafe(fiberId, annotations) {
		if (this._exit) return;
		let cause = causeInterrupt(fiberId);
		if (this.cache.stackFrame) cause = causeAnnotate(cause, make$13(StackTraceKey, this.cache.stackFrame));
		if (annotations) cause = causeAnnotate(cause, annotations);
		this._interruptedCause = this._interruptedCause ? causeAddInterrupts(this._interruptedCause, cause) : cause;
		if (this.interruptible) {
			if (this._running) this._deferredInterrupt = true;
			else this.evaluate(failCause$3(this._interruptedCause));
		}
	}
	pollUnsafe() {
		return this._exit;
	}
	evaluate(effect) {
		if (this._exit) return;
		else if (this._asyncContext !== void 0) {
			const asyncContext = this._asyncContext;
			this._asyncContext = void 0;
			try {
				return asyncContext.runInAsyncScope(this.evaluate, this, effect);
			} finally {
				asyncContext.emitDestroy();
			}
		} else if (this._yielded !== void 0) {
			const yielded = this._yielded;
			this._yielded = void 0;
			yielded();
		}
		const exit = this.runLoop(effect);
		if (exit === Yield) {
			this._asyncContext = captureAsyncContext();
			return;
		}
		const interruptChildren = fiberMiddleware.interruptChildren && fiberMiddleware.interruptChildren(this);
		if (interruptChildren !== void 0) return this.evaluate(flatMap$1(interruptChildren, () => exit));
		this._exit = exit;
		this.cache.runtimeMetrics?.recordFiberEnd(this.context, this._exit);
		if (this._parent) {
			this._parent._children?.delete(this);
			this._parent = void 0;
		}
		if (this._observers !== void 0) {
			const observers = this._observers;
			this._observers = void 0;
			for (let i = 0; i < observers.length; i++) observers[i](exit);
		}
		this._stack.length = 0;
		this._children = void 0;
		this.context = empty$1();
		const cache = this.cache;
		if (cache.span !== void 0 || cache.stackFrame !== void 0) this.cache = {
			...cache,
			span: void 0,
			stackFrame: void 0
		};
	}
	runLoop(effect) {
		const prevFiber = globalThis[currentFiberTypeId];
		globalThis[currentFiberTypeId] = this;
		const prevRunning = this._running;
		this._running = true;
		let yielding = false;
		let current = effect;
		this.currentOpCount = 0;
		try {
			while (true) try {
				if (this._deferredInterrupt) {
					this._deferredInterrupt = false;
					current = failCause$3(this._interruptedCause);
				}
				this.currentOpCount++;
				const cache = this.cache;
				if (!yielding && !cache.preventYield && cache.scheduler.shouldYield(this)) {
					yielding = true;
					const prev = current;
					current = flatMap$1(yieldNow, () => prev);
				}
				current = cache.tracerContext ? cache.tracerContext(current, this) : current[evaluate](this);
				if (current === Yield) {
					const yielded = this._yielded;
					if ("~effect/Exit" in yielded) {
						this._deferredInterrupt = false;
						this._yielded = void 0;
						return yielded;
					} else if (this._deferredInterrupt) {
						this._yielded = void 0;
						yielded();
						continue;
					}
					return Yield;
				}
			} catch (error) {
				if (!hasProperty(current, "~effect/Effect/evaluate")) return exitDie(`Fiber.runLoop: Not a valid effect: ${String(current)}`);
				current = exitDie(error);
			}
		} finally {
			this._running = prevRunning;
			globalThis[currentFiberTypeId] = prevFiber;
		}
	}
	getCont(symbol) {
		if (this._deferredInterrupt) {
			this._deferredInterrupt = false;
			return deferredInterruptCont;
		}
		while (true) {
			const op = this._stack.pop();
			if (!op) return void 0;
			const all = op[contAll];
			if (all !== void 0) {
				const cont = all.call(op, this);
				if (cont) {
					cont[symbol] = cont;
					return cont;
				}
			}
			if (op[symbol]) return op;
		}
	}
	succeedWith(value) {
		if ((++this.currentOpCount & maxInlineSteps - 1) === 0) return exitSucceed(value);
		const cont = this.getCont(contA);
		return cont ? cont[contA](value, this) : this.yieldWith(exitSucceed(value));
	}
	yieldWith(value) {
		this._yielded = value;
		return Yield;
	}
	children() {
		return this._children ??= /* @__PURE__ */ new Set();
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
	setContext(context) {
		const previous = this.context;
		this.context = context;
		if (previous !== void 0 && hasSameCache(previous, context)) return;
		const root = context.cacheRoot;
		const cache = root._fiberCache ??= makeFiberContextCache(context);
		if (this.cache?.scheduler !== cache.scheduler) this._dispatcher = void 0;
		this.cache = cache;
	}
	get currentSpanLocal() {
		const span = this.cache.span;
		return span?._tag === "Span" ? span : void 0;
	}
};
const makeFiberContextCache = (context) => {
	const currentTracer = getOrUndefinedUnsafe(context, TracerKey);
	return {
		scheduler: get(context, Scheduler),
		tracer: currentTracer,
		tracerContext: currentTracer ? currentTracer["context"] : void 0,
		tracerEnabled: get(context, TracerEnabled),
		span: getOrUndefinedUnsafe(context, ParentSpanKey),
		logLevel: get(context, CurrentLogLevel),
		minimumLogLevel: get(context, MinimumLogLevel),
		stackFrame: get(context, CurrentStackFrame),
		runtimeMetrics: getOrUndefinedUnsafe(context, FiberRuntimeMetricsKey),
		maxOpsBeforeYield: get(context, MaxOpsBeforeYield),
		preventYield: get(context, PreventSchedulerYield)
	};
};
const deferredInterruptCont = {
	[contA](_value, fiber) {
		return failCause$3(fiber._interruptedCause);
	},
	[contE](_cause, fiber) {
		return failCause$3(fiber._interruptedCause);
	}
};
const maxInlineSteps = 32;
const fiberMiddleware = { interruptChildren: void 0 };
const fiberStackAnnotations = (fiber) => {
	if (!fiber.cache.stackFrame) return void 0;
	const annotations = /* @__PURE__ */ new Map();
	annotations.set(InterruptorStackTrace.key, fiber.cache.stackFrame);
	return makeUnsafe$5(annotations);
};
/** @internal */
const fiberAwait = (self) => {
	const impl = self;
	if (impl._exit) return succeed$3(impl._exit);
	return callback$1((resume) => {
		if (impl._exit) return resume(succeed$3(impl._exit));
		return sync$1(self.addObserver((exit) => resume(succeed$3(exit))));
	});
};
/** @internal */
const fiberAwaitAll = (self) => callback$1((resume) => {
	const iter = self[Symbol.iterator]();
	const exits = [];
	let cancel = void 0;
	function loop() {
		let result = iter.next();
		while (!result.done) {
			if (result.value._exit) {
				exits.push(result.value._exit);
				result = iter.next();
				continue;
			}
			cancel = result.value.addObserver((exit) => {
				exits.push(exit);
				loop();
			});
			return;
		}
		resume(succeed$3(exits));
	}
	loop();
	return sync$1(() => cancel?.());
});
/** @internal */
const fiberInterrupt = (self) => withFiber$1((fiber) => fiberInterruptAs(self, fiber.id));
/** @internal */
const fiberInterruptAs = /*#__PURE__*/ dual((args) => hasProperty(args[0], FiberTypeId), (self, fiberId, annotations) => withFiber$1((parent) => {
	let ann = fiberStackAnnotations(parent);
	ann = ann && annotations ? merge(ann, annotations) : ann ?? annotations;
	self.interruptUnsafe(fiberId, ann);
	return asVoid$1(fiberAwait(self));
}));
/** @internal */
const fiberInterruptAll = (fibers) => withFiber$1((parent) => {
	const annotations = fiberStackAnnotations(parent);
	let fiberArr = empty();
	for (const fiber of fibers) {
		fiber.interruptUnsafe(parent.id, annotations);
		fiberArr.push(fiber);
	}
	return asVoid$1(fiberAwaitAll(fiberArr));
});
/** @internal */
const succeed$3 = exitSucceed;
/** @internal */
const failCause$3 = exitFailCause;
/** @internal */
const fail$3 = exitFail;
/** @internal */
const sync$1 = /*#__PURE__*/ makePrimitive({
	op: "Sync",
	[evaluate](fiber) {
		const thunk = this[args];
		const value = thunk();
		const cont = fiber.getCont(contA);
		return cont ? cont[contA](value, fiber) : fiber.yieldWith(exitSucceed(value));
	}
});
/** @internal */
const suspend$1 = /*#__PURE__*/ makePrimitive({
	op: "Suspend",
	[evaluate](_fiber) {
		const thunk = this[args];
		return thunk();
	}
});
/** @internal */
const yieldNow = /*#__PURE__*/ (/* @__PURE__ */ makePrimitive({
	op: "Yield",
	[evaluate](fiber) {
		let resumed = false;
		fiber.currentDispatcher.scheduleTask(() => {
			if (resumed) return;
			fiber.evaluate(exitVoid);
		}, this["~effect/Effect/args"] ?? 0);
		return fiber.yieldWith(() => {
			resumed = true;
		});
	}
}))(0);
/** @internal */
const succeedNone = /*#__PURE__*/ succeed$3(/*#__PURE__*/ none());
/** @internal */
const failCauseSync$1 = (evaluate) => suspend$1(() => failCause$3(evaluate()));
/** @internal */
const die$2 = (defect) => exitDie(defect);
/** @internal */
const void_$2 = /*#__PURE__*/ succeed$3(void 0);
/** @internal */
const tryPromise$1 = (options) => {
	const f = typeof options === "function" ? options : options.try;
	const catcher = typeof options === "function" ? (cause) => new UnknownError(cause, "An error occurred in Effect.tryPromise") : options.catch;
	return callbackOptions(function(resume, signal) {
		const failWithCatch = (cause) => suspend$1(() => {
			try {
				return fail$3(internalCall(() => catcher(cause)));
			} catch (err) {
				return die$2(err);
			}
		});
		try {
			f(signal).then((a) => resume(succeed$3(a)), (e) => resume(failWithCatch(e)));
		} catch (err) {
			resume(failWithCatch(err));
		}
	}, f.length !== 0);
};
const callbackOptions = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "Async",
		[evaluate](fiber) {
			let resumed = false;
			let yielded = false;
			const controller = this.withSignal ? new AbortController() : void 0;
			const onCancel = this.register.call(fiber.cache.scheduler, (effect) => {
				if (resumed) return;
				resumed = true;
				if (yielded) fiber.evaluate(effect);
				else yielded = effect;
			}, controller?.signal);
			if (yielded !== false) return yielded;
			yielded = true;
			fiber.yieldWith(() => {
				resumed = true;
			});
			if (controller === void 0 && onCancel === void 0) return Yield;
			fiber._stack.push(asyncFinalizer(() => {
				resumed = true;
				controller?.abort();
				return onCancel ?? exitVoid;
			}));
			return Yield;
		}
	});
	const AsyncImpl = function(register, withSignal) {
		this.register = register;
		this.withSignal = withSignal;
	};
	AsyncImpl.prototype = Proto;
	return function(register, withSignal) {
		return new AsyncImpl(register, withSignal);
	};
}();
const asyncFinalizer = /*#__PURE__*/ makePrimitive({
	op: "AsyncFinalizer",
	[contAll](fiber) {
		if (fiber.interruptible) {
			fiber.interruptible = false;
			fiber._stack.push(setInterruptibleTrue);
		}
	},
	[contE](cause, _fiber) {
		return hasInterrupts(cause) ? flatMap$1(combineFinalizerCause(exitFailCause(cause), this[args]()), () => failCause$3(cause)) : failCause$3(cause);
	}
});
/** @internal */
const callback$1 = (register) => callbackOptions(register, register.length >= 2);
/** @internal */
const never = /*#__PURE__*/ callback$1(constVoid);
/** @internal */
const gen$1 = (...args) => {
	if (args.length === 1) {
		const body = args[0];
		return suspend$1(() => fromIteratorUnsafe(body()));
	}
	const [options, body] = args;
	return suspend$1(() => fromIteratorUnsafe(body.call(options.self)));
};
const fnBody = (bodyOrOptions, pipeables) => typeof bodyOrOptions === "function" ? bodyOrOptions : pipeables.shift().bind(bodyOrOptions.self);
/** @internal */
const fnUntraced = (bodyOrOptions, ...pipeables) => {
	const body = fnBody(bodyOrOptions, pipeables);
	const fn = pipeables.length === 0 ? function() {
		return suspend$1(() => fromIteratorUnsafe(body.apply(this, arguments)));
	} : function() {
		let effect = suspend$1(() => fromIteratorUnsafe(body.apply(this, arguments)));
		for (let i = 0; i < pipeables.length; i++) effect = pipeables[i](effect, ...arguments);
		return effect;
	};
	return defineFunctionLength(body.length, fn);
};
const defineFunctionLength = (length, fn) => Object.defineProperty(fn, "length", {
	value: length,
	configurable: true
});
/** @internal */
const fnUntracedEager$1 = (bodyOrOptions, ...pipeables) => {
	const body = fnBody(bodyOrOptions, pipeables);
	return defineFunctionLength(body.length, pipeables.length === 0 ? function() {
		return fromIteratorEagerUnsafe(() => body.apply(this, arguments));
	} : function() {
		let effect = fromIteratorEagerUnsafe(() => body.apply(this, arguments));
		for (const pipeable of pipeables) effect = pipeable(effect, ...arguments);
		return effect;
	});
};
const fromIteratorEagerUnsafe = (evaluate) => {
	try {
		const iterator = evaluate();
		let value = void 0;
		while (true) {
			const state = iterator.next(value);
			if (state.done) return succeed$3(state.value);
			const primitive = state.value;
			if (primitive && primitive._tag === "Success") {
				value = primitive.value;
				continue;
			} else if (primitive && primitive._tag === "Failure") return state.value;
			else {
				let isFirstExecution = true;
				return suspend$1(() => {
					if (isFirstExecution) {
						isFirstExecution = false;
						return flatMap$1(state.value, (value) => fromIteratorUnsafe(iterator, value));
					} else return suspend$1(() => fromIteratorUnsafe(evaluate()));
				});
			}
		}
	} catch (error) {
		return die$2(error);
	}
};
const fromIteratorUnsafe = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "Iterator",
		[contA](value, fiber) {
			const iter = this.iterator;
			while (true) {
				const state = iter.next(value);
				if (state.done) return succeed$3(state.value);
				if (!effectIsExit(state.value)) {
					fiber._stack.push(this);
					return state.value;
				} else if (state.value._tag === "Failure") return state.value;
				value = state.value.value;
			}
		},
		[evaluate](fiber) {
			return this[contA](this.initial, fiber);
		}
	});
	const IteratorImpl = function(iterator, initial) {
		this.iterator = iterator;
		this.initial = initial;
	};
	IteratorImpl.prototype = Proto;
	return function(iterator, initial) {
		return new IteratorImpl(iterator, initial);
	};
}();
/** @internal */
const as$1 = /*#__PURE__*/ dual(2, (self, value) => new ContImpl(self, returnPayload, succeed$3(value)));
const evaluateCont = function(fiber) {
	fiber._stack.push(this);
	return this[args];
};
const OnSuccessProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnSuccess",
	[evaluate]: evaluateCont
});
const ContImpl = function(self, cont, payload) {
	this[args] = self;
	this[contA] = cont;
	this.payload = payload;
};
ContImpl.prototype = OnSuccessProto;
const returnPayload = function() {
	return this.payload;
};
const succeedPayload = function(_value, fiber) {
	return fiber.succeedWith(this.payload);
};
const continuationMarksStack = /*#__PURE__*/ (() => {
	const limit = /*#__PURE__*/ getStackTraceLimit();
	if (limit === 0) return false;
	const marker = "~effect/Effect/stackProbe";
	const probe = { [marker]: function stackProbe() {
		return /* @__PURE__ */ new Error();
	} };
	setStackTraceLimit(1);
	const error = /*#__PURE__*/ probe[marker]();
	setStackTraceLimit(limit);
	return error.stack?.includes("[as " + marker + "]") === true;
})();
const mapCont = function(value, fiber) {
	const f = this.payload;
	return fiber.succeedWith(continuationMarksStack ? f(value) : internalCall(() => f(value)));
};
const andThenCont = function(value) {
	const f = this.payload;
	return f(value);
};
const tapCont = function(value) {
	const f = this.payload;
	return new ContImpl(f(value), succeedPayload, value);
};
const tapEffectCont = function(value) {
	return new ContImpl(this.payload, succeedPayload, value);
};
/** @internal */
const asSome = (self) => map$2(self, some);
/** @internal */
const andThen$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? returnPayload : andThenCont, f));
/** @internal */
const tap$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? tapEffectCont : tapCont, f));
/** @internal */
const asVoid$1 = (self) => new ContImpl(self, returnPayload, exitVoid);
/** @internal */
const flatMap$1 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, andThenCont, f));
/** @internal */
const effectIsExit = (effect) => effect[ExitTypeId] !== void 0;
/** @internal */
const flatMapEager$1 = /*#__PURE__*/ dual(2, (self, f) => {
	if (effectIsExit(self)) return self._tag === "Success" ? f(self.value) : self;
	return flatMap$1(self, f);
});
/** @internal */
const map$2 = /*#__PURE__*/ dual(2, (self, f) => new ContImpl(self, mapCont, f));
/** @internal */
const mapEager$1 = /*#__PURE__*/ dual(2, (self, f) => effectIsExit(self) ? exitMap(self, f) : map$2(self, f));
/** @internal */
const exitIsSuccess = (self) => self._tag === "Success";
/** @internal */
const exitVoid = /*#__PURE__*/ exitSucceed(void 0);
/** @internal */
const exitMap = /*#__PURE__*/ dual(2, (self, f) => self._tag === "Success" ? exitSucceed(f(self.value)) : self);
/** @internal */
const exitAsVoidAll = (exits) => {
	const failures = [];
	for (const exit of exits) if (exit._tag === "Failure") failures.push(...exit.cause.reasons);
	return failures.length === 0 ? exitVoid : exitFailCause(causeFromReasons(failures));
};
/** @internal */
const updateContext = /*#__PURE__*/ dual(2, (self, f) => withFiber$1((fiber) => {
	const prevContext = fiber.context;
	const nextContext = f(prevContext);
	if (prevContext === nextContext) return self;
	fiber.setContext(nextContext);
	onExitUnsafe(fiber, () => {
		fiber.setContext(prevContext);
	});
	return self;
}));
/** @internal */
const contextWith$1 = (f) => withFiber$1((fiber) => f(fiber.context));
/** @internal */
const provideContext = /*#__PURE__*/ dual(2, (self, context) => {
	if (effectIsExit(self)) return self;
	return updateContext(self, merge(context));
});
/** @internal */
const provideService = function() {
	if (arguments.length === 1) return dual(2, (self, impl) => provideServiceImpl(self, arguments[0], impl));
	return dual(3, (self, service, impl) => provideServiceImpl(self, service, impl)).apply(this, arguments);
};
const provideServiceImpl = (self, service, implementation) => updateContext(self, add(service, implementation));
/** @internal */
const when$1 = /*#__PURE__*/ dual(2, (self, condition) => flatMap$1(condition, (pass) => pass ? asSome(self) : succeedNone));
/** @internal */
const forever$2 = /*#__PURE__*/ dual((args) => isEffect(args[0]), (self, options) => whileLoop$1({
	while: constTrue,
	body: constant(options?.disableYield ? self : flatMap$1(self, (_) => yieldNow)),
	step: constVoid
}));
/** @internal */
const catchCause$1 = /*#__PURE__*/ dual(2, (self, f) => new OnFailureImpl(self, f.length !== 1 ? (cause) => f(cause) : f));
const OnFailureProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnFailure",
	[evaluate]: evaluateCont
});
const OnFailureImpl = function(self, f) {
	this[args] = self;
	this[contE] = f;
};
OnFailureImpl.prototype = OnFailureProto;
/** @internal */
const catchCauseFilter = /*#__PURE__*/ dual(3, (self, filter, f) => catchCause$1(self, (cause) => {
	const eb = filter(cause);
	return isFailure(eb) ? failCause$3(eb.failure) : f(eb.success, cause);
}));
/** @internal */
const ignoreCauseLogger = (log) => log === "Unhandled" ? (...message) => withFiber$1((fiber) => {
	const level = fiber.getRef(UnhandledLogLevel);
	return level === void 0 ? void_$2 : logWithLevel(level)(...message);
}) : logWithLevel(log === true ? void 0 : log);
/** @internal */
const ignore$1 = /*#__PURE__*/ dual((args) => isEffect(args[0]), (self, options) => {
	if (!options?.log) return matchEffect$1(self, {
		onFailure: (_) => void_$2,
		onSuccess: (_) => void_$2
	});
	const logEffect = ignoreCauseLogger(options.log);
	return matchCauseEffect(self, {
		onFailure(cause) {
			const error = findRecoverableError(cause);
			return isFailure(error) ? failCause$3(error.failure) : options.message === void 0 ? logEffect(cause) : logEffect(options.message, cause);
		},
		onSuccess: (_) => void_$2
	});
});
/** @internal */
const matchCauseEffect = /*#__PURE__*/ dual(2, (self, options) => new OnSuccessAndFailureImpl(self, options.onSuccess.length !== 1 ? (a) => options.onSuccess(a) : options.onSuccess, options.onFailure.length !== 1 ? (cause) => options.onFailure(cause) : options.onFailure));
const OnSuccessAndFailureProto = /*#__PURE__*/ makePrimitiveProto({
	op: "OnSuccessAndFailure",
	[evaluate]: evaluateCont
});
const OnSuccessAndFailureImpl = function(self, onSuccess, onFailure) {
	this[args] = self;
	this[contA] = onSuccess;
	this[contE] = onFailure;
};
OnSuccessAndFailureImpl.prototype = OnSuccessAndFailureProto;
/** @internal */
const matchEffect$1 = /*#__PURE__*/ dual(2, (self, options) => matchCauseEffect(self, {
	onFailure: (cause) => {
		const error = findRecoverableError(cause);
		return isFailure(error) ? failCause$3(error.failure) : options.onFailure(error.success);
	},
	onSuccess: options.onSuccess
}));
/** @internal */
const exit$1 = (self) => effectIsExit(self) ? exitSucceed(self) : exitPrimitive(self);
const exitPrimitive = /*#__PURE__*/ makePrimitive({
	op: "Exit",
	[evaluate](fiber) {
		fiber._stack.push(this);
		return this[args];
	},
	[contA](value, fiber, exit) {
		return fiber.succeedWith(exit ?? exitSucceed(value));
	},
	[contE](cause, fiber, exit) {
		return fiber.succeedWith(exit ?? exitFailCause(cause));
	}
});
/** @internal */
const ScopeTypeId = "~effect/Scope";
/** @internal */
const ScopeCloseableTypeId = "~effect/Scope/Closeable";
/** @internal */
const scopeTag = /*#__PURE__*/ Service("effect/Scope");
/** @internal */
const scopeClose = (self, exit_) => withFiber$1((fiber) => {
	fiberEnterUninterruptibleUnsafe(fiber);
	return scopeCloseUnsafe(self, exit_) ?? void_$2;
});
/** @internal */
const scopeCloseUnsafe = (self, exit_) => {
	const state = self.state;
	if (state._tag === "Closed") return;
	self.state = {
		_tag: "Closed",
		exit: exit_
	};
	if (self.parent !== void 0) scopeRemoveFinalizerUnsafe(self.parent, self);
	if (state._tag === "Empty") return;
	if (state.finalizer !== void 0) return runFinalizer(state.finalizer, exit_);
	const finalizers = state.finalizers;
	if (finalizers.size === 1) return runFinalizer(finalizers.values().next().value, exit_);
	return scopeCloseFinalizers(self, finalizers, exit_);
};
const runFinalizer = (finalizer, exit_) => {
	try {
		return finalizer(exit_);
	} catch (defect) {
		return exitDie(defect);
	}
};
const combineFinalizerCause = (exit_, finalizer) => exitIsSuccess(exit_) ? finalizer : catchCause$1(finalizer, (cause) => failCause$3(causeElectDefects(causeCombine(exit_.cause, cause))));
const scopeCloseFinalizers = /*#__PURE__*/ fnUntraced(function* (self, finalizers, exit_) {
	let exits = [];
	const fibers = [];
	const arr = Array.from(finalizers);
	const parent = getCurrentFiber();
	for (let i = arr.length - 1; i >= 0; i--) {
		const [key, finalizer] = arr[i];
		if (self.strategy === "sequential") exits.push(yield* exit$1(runFinalizer(finalizer, exit_)));
		else if (key !== parent) fibers.push(forkUnsafe$1(parent, runFinalizer(finalizer, exit_), true, true, "inherit"));
	}
	if (fibers.length > 0) exits = yield* fiberAwaitAll(fibers);
	return yield* exitAsVoidAll(exits);
});
/** @internal */
const scopeForkUnsafe = (scope, finalizerStrategy) => {
	const child = makeScope(finalizerStrategy, scope);
	if (scope.state._tag === "Closed") {
		child.state = scope.state;
		return child;
	}
	scopeAddFinalizerUnsafe(scope, child, (exit) => scopeClose(child, exit));
	return child;
};
/** @internal */
const scopeAddFinalizerExit = (scope, finalizer) => {
	return suspend$1(() => {
		if (scope.state._tag === "Closed") return finalizer(scope.state.exit);
		scopeAddFinalizerUnsafe(scope, {}, finalizer);
		return void_$2;
	});
};
/** @internal */
const scopeAddFinalizer = (scope, finalizer) => scopeAddFinalizerExit(scope, constant(finalizer));
/** @internal */
const scopeAddFinalizerUnsafe = (scope, key, finalizer) => {
	if (scope.state._tag === "Empty") scope.state = {
		_tag: "Open",
		finalizerKey: key,
		finalizer,
		finalizers: void 0
	};
	else if (scope.state._tag === "Open") {
		const state = scope.state;
		if (state.finalizer !== void 0) {
			state.finalizers = /* @__PURE__ */ new Map([[state.finalizerKey, state.finalizer]]);
			state.finalizerKey = void 0;
			state.finalizer = void 0;
			state.finalizers.set(key, finalizer);
		} else state.finalizers.set(key, finalizer);
	}
};
/** @internal */
const scopeRemoveFinalizerUnsafe = (scope, key) => {
	if (scope.state._tag !== "Open") return;
	const state = scope.state;
	if (state.finalizerKey === key) scope.state = constScopeEmpty;
	else if (state.finalizers !== void 0) {
		state.finalizers.delete(key);
		if (state.finalizers.size === 0) scope.state = constScopeEmpty;
	}
};
/** @internal */
const scopeMakeUnsafe = (finalizerStrategy) => makeScope(finalizerStrategy, void 0);
const makeScope = (finalizerStrategy = "sequential", parent) => ({
	[ScopeCloseableTypeId]: ScopeCloseableTypeId,
	[ScopeTypeId]: ScopeTypeId,
	strategy: finalizerStrategy,
	parent,
	state: constScopeEmpty
});
const constScopeEmpty = { _tag: "Empty" };
/** @internal */
const scope = scopeTag;
/** @internal */
const provideScope = /*#__PURE__*/ provideService(scopeTag);
/** @internal */
const acquireRelease$1 = (acquire, release, options) => contextWith$1((context) => uninterruptibleMask((restore) => flatMap$1(scope, (scope) => tap$1(options?.interruptible ? restore(acquire) : acquire, (a) => scopeAddFinalizerExit(scope, (exit) => provideContext(release(a, exit), context))))));
/** @internal */
const addFinalizer$2 = (finalizer) => flatMap$1(scope, (scope) => contextWith$1((context) => scopeAddFinalizerExit(scope, (exit) => provideContext(finalizer(exit), context))));
const OnExitImpl = /*#__PURE__*/ function() {
	const Proto = /*#__PURE__*/ makePrimitiveProto({
		op: "OnExit",
		[evaluate](fiber) {
			fiber._stack.push(this);
			return this.effect;
		},
		[contAll](fiber) {
			if (fiber.interruptible && this.interruptible !== true) {
				fiber._stack.push(setInterruptibleTrue);
				fiber.interruptible = false;
			}
		},
		[contA](value, _, exit) {
			exit ??= exitSucceed(value);
			let eff;
			try {
				eff = this.onExit(exit);
			} catch (defect) {
				eff = exitDie(defect);
			}
			return eff ? flatMap$1(eff, (_) => exit) : exit;
		},
		[contE](cause, _, exit) {
			exit ??= exitFailCause(cause);
			let eff;
			try {
				eff = this.onExit(exit);
			} catch (defect) {
				eff = exitDie(defect);
			}
			return eff ? flatMap$1(combineFinalizerCause(exit, eff), (_) => exit) : exit;
		}
	});
	const OnExitImpl = function(effect, onExit, interruptible) {
		this.effect = effect;
		this.onExit = onExit;
		this.interruptible = interruptible;
	};
	OnExitImpl.prototype = Proto;
	return OnExitImpl;
}();
/** @internal */
const onExitPrimitive = (effect, onExit, interruptible) => new OnExitImpl(effect, onExit, interruptible);
/**
* Pushes an exit finalizer for the current `withFiber` evaluation. Only call inside `withFiber`.
* @internal
*/
const onExitUnsafe = (fiber, f) => {
	fiber._stack.push(new OnExitImpl(void 0, f, void 0));
};
/** @internal */
const onExit$1 = /*#__PURE__*/ dual(2, onExitPrimitive);
/** @internal */
const onErrorFilter = /*#__PURE__*/ dual(3, (self, filter, f) => onExit$1(self, (exit) => {
	if (exit._tag !== "Failure") return void_$2;
	const result = filter(exit.cause);
	return isFailure(result) ? void_$2 : f(result.success, exit.cause);
}));
/** @internal */
const onInterrupt$1 = /*#__PURE__*/ dual(2, (self, finalizer) => onErrorFilter(causeFilterInterruptors, finalizer)(self));
/** @internal */
const interrupt$1 = /*#__PURE__*/ withFiber$1((fiber) => failCause$3(causeInterrupt(fiber.id)));
/** @internal */
const uninterruptible$1 = (self) => withFiber$1((fiber) => {
	if (!fiber.interruptible) return self;
	fiber.interruptible = false;
	fiber._stack.push(setInterruptibleTrue);
	return self;
});
const setInterruptible = /*#__PURE__*/ makePrimitive({
	op: "SetInterruptible",
	[contAll](fiber) {
		fiber.interruptible = this[args];
		if (fiber._interruptedCause && fiber.interruptible) return () => failCause$3(fiber._interruptedCause);
	}
});
const setInterruptibleTrue = /*#__PURE__*/ setInterruptible(true);
const setInterruptibleFalse = /*#__PURE__*/ setInterruptible(false);
const setFiberInterruptible = (fiber) => {
	fiber.interruptible = true;
	fiber._stack.push(setInterruptibleFalse);
	if (fiber._interruptedCause) return failCause$3(fiber._interruptedCause);
};
/**
* Makes the current fiber uninterruptible for the returned effect without an
* extra primitive. Call only within `withFiber`.
*
* @internal
*/
const fiberEnterUninterruptibleUnsafe = (fiber) => {
	const impl = fiber;
	if (!impl.interruptible) return;
	impl.interruptible = false;
	impl._stack.push(setInterruptibleTrue);
};
/** @internal */
const interruptible = (self) => withFiber$1((fiber) => {
	if (fiber.interruptible) return self;
	return setFiberInterruptible(fiber) ?? self;
});
/** @internal */
const uninterruptibleMask = (f) => withFiber$1((fiber) => {
	if (!fiber.interruptible) return f(identity);
	fiber.interruptible = false;
	fiber._stack.push(setInterruptibleTrue);
	return f(interruptible);
});
/** @internal */
const whileLoop$1 = /*#__PURE__*/ makePrimitive({
	op: "While",
	[contA](value, fiber) {
		this[args].step(value);
		if (this["~effect/Effect/args"].while()) {
			fiber._stack.push(this);
			return this[args].body();
		}
		return exitVoid;
	},
	[evaluate](fiber) {
		if (this["~effect/Effect/args"].while()) {
			fiber._stack.push(this);
			return this[args].body();
		}
		return exitVoid;
	}
});
/** @internal */
const forEach$1 = /*#__PURE__*/ dual((args) => typeof args[1] === "function", (iterable, f, options) => suspend$1(() => {
	const concurrency = resolveConcurrency(options?.concurrency);
	if (concurrency === 1) return forEachSequential(iterable, f, options);
	const items = fromIterable$1(iterable);
	let length = items.length;
	if (length === 0) return options?.discard ? void_$2 : succeed$3([]);
	const out = options?.discard ? void 0 : new Array(length);
	const eff = forEachConcurrent({
		f,
		out
	}, items, { concurrency });
	return eff ? as$1(eff, out) : succeed$3(out);
}));
const forEachSequential = (iterable, f, options) => suspend$1(() => {
	const out = options?.discard ? void 0 : [];
	const iterator = iterable[Symbol.iterator]();
	let state = iterator.next();
	let index = 0;
	return as$1(whileLoop$1({
		while: () => !state.done,
		body: () => f(state.value, index++),
		step: (b) => {
			if (out) out.push(b);
			state = iterator.next();
		}
	}), out);
});
/** @internal */
const resolveConcurrency = (concurrency) => concurrency === "unbounded" ? Number.POSITIVE_INFINITY : Math.max(1, concurrency ?? 1);
/** @internal */
const iterateEager = () => (options) => {
	const onItem = options.onItem;
	const step = options.step;
	const resumeSequential = (state, items, index, end, effect) => flatMap$1(exit$1(effect), (itemExit) => step(state, items[index], itemExit, index) ?? runSequential(state, items, index + 1, end) ?? void_$2);
	const runSequential = (state, items, index = 0, end = items.length) => {
		for (; index < end; index++) {
			const item = items[index];
			const effect = onItem(state, item, index);
			if (!effectIsExit(effect)) return resumeSequential(state, items, index, end, effect);
			const terminal = step(state, item, effect, index);
			if (terminal) return terminal._tag === "Failure" ? terminal : void 0;
		}
	};
	return runSequential;
};
const iterateConcurrentImpl = (options) => {
	const onItem = options.onItem;
	const step = options.step;
	return (state, items, opts) => {
		let index = 0;
		const end = opts.end ?? items.length;
		const concurrency = opts.concurrency;
		let done = false;
		let parentFiber;
		let fibers;
		let resume;
		let terminal;
		let effect;
		const failDefect = (error) => {
			const defect = exitDie(error);
			terminal = defect;
			done = true;
			return fibers && fibers.size > 0 ? flatMap$1(uninterruptible$1(fiberInterruptAll(Array.from(fibers))), () => terminal ?? defect) : defect;
		};
		const go = () => {
			let paused = false;
			for (; !terminal && index < end; index++) {
				const item = items[index];
				const eff = effect ?? onItem(state, item, index);
				if (effectIsExit(eff)) {
					terminal = step(state, item, eff, index);
					if (terminal) break;
				} else if (!parentFiber) return callback$1((cb) => {
					parentFiber = getCurrentFiber();
					fibers = /* @__PURE__ */ new Set();
					effect = eff;
					resume = cb;
					let result;
					try {
						result = go();
					} catch (error) {
						return cb(failDefect(error));
					}
					if (result) return cb(result);
					return suspend$1(() => {
						terminal ??= exitVoid;
						return flatMap$1(fibers ? fiberInterruptAll(fibers) : void_$2, () => terminal?._tag === "Failure" ? terminal : void_$2);
					});
				});
				else {
					effect = void 0;
					const fiber = forkUnsafe$1(parentFiber, eff, true, true, "inherit");
					if (fiber._exit) {
						terminal = step(state, item, fiber._exit, index);
						if (terminal) break;
						continue;
					}
					fibers.add(fiber);
					const currentIndex = index;
					fiber.addObserver((exit) => {
						fibers.delete(fiber);
						try {
							if (terminal) {
								if (exit._tag === "Failure") {
									const reasons = exit.cause.reasons.filter((reason) => reason._tag !== "Interrupt");
									if (reasons.length > 0) {
										const cause = causeFromReasons(reasons);
										terminal = exitFailCause(terminal._tag === "Failure" ? causeElectDefects(causeCombine(terminal.cause, cause)) : cause);
									}
								}
							} else {
								const result = step(state, item, exit, currentIndex);
								if (result) {
									terminal = result;
									go();
								}
							}
							if (paused) {
								const eff = go();
								if (eff) resume(eff);
							} else if (done && fibers.size === 0) resume(terminal ?? void_$2);
						} catch (error) {
							resume(failDefect(error));
						}
					});
					if (fibers.size < concurrency) continue;
					paused = true;
					index++;
					return;
				}
			}
			done = true;
			if (terminal) {
				if (fibers && fibers.size > 0) {
					const annotations = fiberStackAnnotations(parentFiber);
					fibers.forEach((f) => f.interruptUnsafe(parentFiber.id, annotations));
					return;
				}
				if (resume || terminal._tag === "Failure") return terminal;
			} else if (resume) {
				if (!fibers) return exitVoid;
				else if (fibers.size === 0) resume(void_$2);
			}
		};
		return go();
	};
};
/** @internal */
const iterateConcurrent = () => (options) => iterateConcurrentImpl(options);
const forEachConcurrent = /*#__PURE__*/ iterateConcurrentImpl({
	onItem(state, item, index) {
		return state.f(item, index);
	},
	step(state, _, exit, index) {
		if (exit._tag === "Failure") return exit;
		else if (state.out) state.out[index] = exit.value;
	}
});
/** @internal */
const forkUnsafe$1 = (parent, effect, immediate = false, daemon = false, uninterruptible = false) => {
	const parentRuntime = parent;
	const interruptible = uninterruptible === "inherit" ? parentRuntime.interruptible : !uninterruptible;
	const child = new FiberImpl(parentRuntime.context, interruptible);
	if (!daemon) {
		parentRuntime.children().add(child);
		child._parent = parentRuntime;
	}
	if (immediate) child.evaluate(effect);
	else {
		child._asyncContext = captureAsyncContext();
		parentRuntime.currentDispatcher.scheduleTask(() => child.evaluate(effect), 0);
	}
	return child;
};
const fiberScopeFinalizer = (fiber) => () => withFiber$1((current) => current === fiber ? void_$2 : fiberInterrupt(fiber));
/** @internal */
const forkIn = /*#__PURE__*/ dual((args) => isEffect(args[0]), (self, scope, options) => withFiber$1((parent) => {
	const fiber = scope.state._tag === "Closed" ? new FiberImpl(parent.context) : forkUnsafe$1(parent, self, options?.startImmediately, true, options?.uninterruptible);
	if (!fiber._exit) {
		if (scope.state._tag !== "Closed") {
			scopeAddFinalizerUnsafe(scope, fiber, fiberScopeFinalizer(fiber));
			fiber.addObserver(() => scopeRemoveFinalizerUnsafe(scope, fiber));
		} else fiber.interruptUnsafe(parent.id, fiberStackAnnotations(parent));
	}
	return succeed$3(fiber);
}));
/** @internal */
const forkScoped$1 = /*#__PURE__*/ dual((args) => isEffect(args[0]), (self, options) => flatMap$1(scope, (scope) => forkIn(self, scope, options)));
/** @internal */
const runForkWith = (context) => (effect, options) => {
	const fiber = new FiberImpl(options?.scheduler ? add(context, Scheduler, options.scheduler) : context, options?.uninterruptible !== true);
	fiber.evaluate(effect);
	if (fiber._exit) return fiber;
	if (options?.signal) {
		if (options.signal.aborted) fiber.interruptUnsafe();
		else {
			const abort = () => fiber.interruptUnsafe();
			options.signal.addEventListener("abort", abort, { once: true });
			fiber.addObserver(() => options.signal.removeEventListener("abort", abort));
		}
	}
	if (options?.onFiberStart) options.onFiberStart(fiber);
	return fiber;
};
/** @internal */
const runFork$1 = /*#__PURE__*/ runForkWith(/*#__PURE__*/ empty$1());
/** @internal */
const runPromiseExitWith = (context) => {
	const runFork = runForkWith(context);
	return (effect, options) => {
		const fiber = runFork(effect, options);
		return new Promise((resolve) => {
			fiber.addObserver((exit) => resolve(exit));
		});
	};
};
/** @internal */
const runPromiseWith = (context) => {
	const runPromiseExit = runPromiseExitWith(context);
	return (effect, options) => runPromiseExit(effect, options).then((exit) => {
		if (exit._tag === "Failure") throw causeSquash(exit.cause);
		return exit.value;
	});
};
/** @internal */
const runPromise$1 = /*#__PURE__*/ runPromiseWith(/*#__PURE__*/ empty$1());
/** @internal */
const runSyncExitWith = (context) => {
	const runFork = runForkWith(context);
	return (effect) => {
		if (effectIsExit(effect)) return effect;
		const scheduler = new MixedScheduler("sync");
		const fiber = runFork(effect, { scheduler });
		fiber._dispatcher?.flush();
		return fiber._exit ?? exitDie(new AsyncFiberError(fiber));
	};
};
/** @internal */
const runSyncExit$1 = /*#__PURE__*/ runSyncExitWith(/*#__PURE__*/ empty$1());
const succeedTrue = /*#__PURE__*/ succeed$3(true);
const succeedFalse = /*#__PURE__*/ succeed$3(false);
var Latch = class {
	waiters = [];
	scheduled = void 0;
	_isOpen;
	constructor(isOpen) {
		this._isOpen = isOpen;
	}
	scheduleUnsafe(fiber) {
		if (this.waiters.length === 0) return succeedTrue;
		if (this.scheduled === void 0) {
			this.scheduled = this.waiters;
			fiber.currentDispatcher.scheduleTask(this.flushScheduled, 0);
		} else for (let i = 0; i < this.waiters.length; i++) this.scheduled.push(this.waiters[i]);
		this.waiters = [];
		return succeedTrue;
	}
	flushScheduled = () => {
		if (this.scheduled === void 0) return;
		const waiters = this.scheduled;
		this.scheduled = void 0;
		for (let i = 0; i < waiters.length; i++) waiters[i](exitVoid);
	};
	flushWaiters() {
		const waiters = this.waiters;
		this.waiters = [];
		this.flushScheduled();
		for (let i = 0; i < waiters.length; i++) waiters[i](exitVoid);
	}
	open = /*#__PURE__*/ withFiber$1((fiber) => {
		if (this._isOpen) return succeedFalse;
		this._isOpen = true;
		return this.scheduleUnsafe(fiber);
	});
	release = /*#__PURE__*/ withFiber$1((fiber) => this._isOpen ? succeedFalse : this.scheduleUnsafe(fiber));
	openUnsafe() {
		if (this._isOpen) return false;
		this._isOpen = true;
		this.flushWaiters();
		return true;
	}
	await = /*#__PURE__*/ callback$1((resume) => {
		if (this._isOpen) return resume(void_$2);
		this.waiters.push(resume);
		return sync$1(() => {
			let index = this.waiters.indexOf(resume);
			if (index !== -1) this.waiters.splice(index, 1);
			else if (this.scheduled !== void 0) {
				index = this.scheduled.indexOf(resume);
				if (index !== -1) this.scheduled.splice(index, 1);
			}
		});
	});
	closeUnsafe() {
		if (!this._isOpen) return false;
		this._isOpen = false;
		return true;
	}
	close = /*#__PURE__*/ sync$1(() => this.closeUnsafe());
	whenOpen = (self) => flatMap$1(this.await, () => self);
	isOpen() {
		return this._isOpen;
	}
};
/** @internal */
const makeLatchUnsafe = (open) => new Latch(open ?? false);
/** @internal */
const ClockRef = /*#__PURE__*/ Reference("effect/Clock", { defaultValue: () => new ClockImpl() });
const MAX_TIMER_MILLIS = 2 ** 31 - 1;
var ClockImpl = class {
	currentTimeMillisUnsafe() {
		return Date.now();
	}
	currentTimeMillis = /*#__PURE__*/ sync$1(() => this.currentTimeMillisUnsafe());
	currentTimeNanosUnsafe() {
		return wallTimeNanos();
	}
	currentTimeNanos = /*#__PURE__*/ sync$1(() => this.currentTimeNanosUnsafe());
	monotonicTimeNanosUnsafe() {
		return monotonicNowNanos();
	}
	monotonicTimeNanos = /*#__PURE__*/ sync$1(() => this.monotonicTimeNanosUnsafe());
	sleep(duration) {
		return this.sleepMillis(toMillis(duration));
	}
	sleepMillis(millis) {
		if (millis <= 0) return yieldNow;
		else if (!Number.isFinite(millis)) return never;
		return callback$1((resume) => {
			const continuation = millis > MAX_TIMER_MILLIS ? this.sleepMillis(millis - MAX_TIMER_MILLIS) : void_$2;
			const handle = setTimeout(() => resume(continuation), Math.min(millis, MAX_TIMER_MILLIS));
			return sync$1(() => clearTimeout(handle));
		});
	}
};
const nanosPerMilli = /*#__PURE__*/ BigInt(1e6);
const monotonicNowNanos = /*#__PURE__*/ function() {
	const processHrtime = globalThis.process?.hrtime;
	if (typeof processHrtime?.bigint === "function") return () => processHrtime.bigint();
	if (typeof performance !== "undefined" && typeof performance.now === "function") return () => BigInt(Math.round(performance.now() * 1e6));
	let previous = /*#__PURE__*/ BigInt(0);
	return () => {
		const current = BigInt(Date.now()) * nanosPerMilli;
		if (current > previous) previous = current;
		return previous;
	};
}();
const wallTimeNanos = /*#__PURE__*/ function() {
	const reanchorThresholdNanos = /*#__PURE__*/ BigInt(1e9);
	let origin;
	return () => {
		const monotonic = monotonicNowNanos();
		const wall = BigInt(Date.now()) * nanosPerMilli;
		if (origin === void 0) origin = wall - monotonic;
		else {
			const projected = origin + monotonic;
			if ((wall > projected ? wall - projected : projected - wall) > reanchorThresholdNanos) origin = wall - monotonic;
		}
		return origin + monotonic;
	};
}();
/** @internal */
const clockWith = (f) => withFiber$1((fiber) => f(fiber.getRef(ClockRef)));
/** @internal */
const sleep = (duration) => clockWith((clock) => clock.sleep(fromInputUnsafe(duration)));
/** @internal */
const AsyncFiberErrorTypeId = "~effect/Cause/AsyncFiberError";
/** @internal */
var AsyncFiberError = class extends (/*#__PURE__*/ TaggedError$2("AsyncFiberError")) {
	[AsyncFiberErrorTypeId] = AsyncFiberErrorTypeId;
	constructor(fiber) {
		super({
			message: "An asynchronous Effect was executed with Effect.runSync",
			fiber
		});
	}
};
/** @internal */
const UnknownErrorTypeId = "~effect/Cause/UnknownError";
/** @internal */
var UnknownError = class extends (/*#__PURE__*/ TaggedError$2("UnknownError")) {
	[UnknownErrorTypeId] = UnknownErrorTypeId;
	constructor(cause, message) {
		super({
			message,
			cause
		});
	}
};
/** @internal */
const ConsoleRef = /*#__PURE__*/ Reference("effect/Console", { defaultValue: () => globalThis.console });
/** @internal */
const logLevelToOrder = (level) => {
	switch (level) {
		case "All": return Number.MIN_SAFE_INTEGER;
		case "Fatal": return 5e4;
		case "Error": return 4e4;
		case "Warn": return 3e4;
		case "Info": return 2e4;
		case "Debug": return 1e4;
		case "Trace": return 0;
		case "None": return Number.MAX_SAFE_INTEGER;
	}
};
/** @internal */
const isLogLevelGreaterThan = /*#__PURE__*/ isGreaterThan$1(/* @__PURE__ */ mapInput(Number$4, logLevelToOrder));
/** @internal */
const CurrentLoggers = /*#__PURE__*/ Reference("effect/Logger/CurrentLoggers", { defaultValue: () => /* @__PURE__ */ new Set([defaultLogger, tracerLogger]) });
/** @internal */
const LogToStderr = /*#__PURE__*/ Reference("effect/Logger/LogToStderr", { defaultValue: constFalse });
const LoggerProto = {
	["~effect/Logger"]: {
		_Message: identity,
		_Output: identity
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/** @internal */
const loggerMake = (log) => {
	const self = Object.create(LoggerProto);
	self.log = log;
	return self;
};
/**
* Sanitize a given string by replacing spaces, equal signs, and double quotes
* with underscores.
*
* @internal
*/
const formatLabel = (key) => key.replace(/[\s="]/g, "_");
/**
* Formats a log span into a `<label>=<value>ms` string.
*
* @internal
*/
const formatLogSpan = (self, now) => {
	return `${formatLabel(self[0])}=${now - self[1]}ms`;
};
/** @internal */
const logWithLevel = (level) => (...message) => {
	let cause = void 0;
	for (let i = 0, len = message.length; i < len; i++) {
		const msg = message[i];
		if (isCause(msg)) {
			if (cause) message.splice(i, 1);
			else message = message.slice(0, i).concat(message.slice(i + 1));
			cause = cause ? causeFromReasons(cause.reasons.concat(msg.reasons)) : msg;
			i--;
		}
	}
	if (cause === void 0) cause = causeEmpty;
	return withFiber$1((fiber) => {
		const logLevel = level ?? fiber.cache.logLevel;
		if (isLogLevelGreaterThan(fiber.cache.minimumLogLevel, logLevel)) return void_$2;
		const clock = fiber.getRef(ClockRef);
		const loggers = fiber.getRef(CurrentLoggers);
		if (loggers.size > 0) {
			const date = new Date(clock.currentTimeMillisUnsafe());
			for (const logger of loggers) logger.log({
				cause,
				fiber,
				date,
				logLevel,
				message
			});
		}
		return void_$2;
	});
};
const colors = {
	bold: "1",
	red: "31",
	green: "32",
	yellow: "33",
	blue: "34",
	cyan: "36",
	white: "37",
	gray: "90",
	black: "30",
	bgBrightRed: "101"
};
colors.gray, colors.blue, colors.green, colors.yellow, colors.red, colors.bgBrightRed, colors.black;
const defaultDateFormat = (date) => `${date.getHours().toString().padStart(2, "0")}:${date.getMinutes().toString().padStart(2, "0")}:${date.getSeconds().toString().padStart(2, "0")}.${date.getMilliseconds().toString().padStart(3, "0")}`;
/** @internal */
const defaultLogger = /*#__PURE__*/ loggerMake(({ cause, date, fiber, logLevel, message }) => {
	const message_ = Array.isArray(message) ? message.slice() : [message];
	if (cause.reasons.length > 0) message_.push(causePretty(cause));
	const now = date.getTime();
	const spans = fiber.getRef(CurrentLogSpans);
	let spanString = "";
	for (const span of spans) spanString += ` ${formatLogSpan(span, now)}`;
	const annotations = fiber.getRef(CurrentLogAnnotations);
	if (Object.keys(annotations).length > 0) message_.push(annotations);
	const console = fiber.getRef(ConsoleRef);
	(fiber.getRef(LogToStderr) ? console.error : console.log)(`[${defaultDateFormat(date)}] ${logLevel.toUpperCase()} (#${fiber.id})${spanString}:`, ...message_);
});
/** @internal */
const tracerLogger = /*#__PURE__*/ loggerMake(({ cause, fiber, logLevel, message }) => {
	const clock = fiber.getRef(ClockRef);
	const annotations = fiber.getRef(CurrentLogAnnotations);
	const span = fiber.cache.span;
	if (span === void 0 || span._tag === "ExternalSpan" || span.status._tag === "Ended") return;
	const attributes = {};
	for (const [key, value] of Object.entries(annotations)) assignProperty(attributes, key, value);
	attributes["effect.fiberId"] = fiber.id;
	attributes["effect.logLevel"] = logLevel.toUpperCase();
	if (cause.reasons.length > 0) attributes["effect.cause"] = causePretty(cause);
	span.event(toStringUnknown(Array.isArray(message) && message.length === 1 ? message[0] : message), clock.currentTimeNanosUnsafe(), attributes);
});
// node_modules/effect/dist/Exit.js
/**
* Creates a successful Exit containing the given value.
*
* **When to use**
*
* Use when you need an Exit that contains a known success value.
*
* **Details**
*
* Returns a `Success<A>` with the provided value. Does not perform any
* computation.
*
* **Example** (Creating a successful Exit)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* Exit.succeed(42) // => Exit.succeed(42)
* ```
*
* @see {@link fail} to create a failed Exit
* @see {@link void_ void} for a pre-allocated success with no value
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const succeed$2 = exitSucceed;
/**
* Creates a failed Exit from a Cause.
*
* **When to use**
*
* Use when you already have a `Cause<E>` and want to wrap it in an Exit
* for advanced error handling where you need full control over the Cause
* structure.
*
* **Details**
*
* Returns a `Failure<never, E>`. If you only have an error value, use
* {@link fail} instead.
*
* **Example** (Creating a failed Exit from a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Exit } from "effect"
*
* Exit.failCause(Cause.fail("Something went wrong")) // => Exit.fail("Something went wrong")
* ```
*
* @see {@link fail} to create a Failure from a plain error value
* @see {@link die} to create a Failure from a defect
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCause$2 = exitFailCause;
/**
* Creates a failed Exit from a typed error value.
*
* **When to use**
*
* Use when you need to represent an expected typed failure as an `Exit`.
*
* **Details**
*
* The error is wrapped in a `Cause.Fail` internally.
*
* Returns a `Failure<never, E>`.
*
* **Example** (Creating a failed Exit)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* Exit.fail("Something went wrong") // => Exit.fail("Something went wrong")
* ```
*
* @see {@link succeed} to create a successful Exit
* @see {@link die} to create a Failure from an unexpected defect
* @see {@link failCause} to create a Failure from a full Cause
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fail$2 = exitFail;
const void_$1 = exitVoid;
/**
* Checks whether an Exit is a Success.
*
* **When to use**
*
* Use as a type guard to narrow `Exit<A, E>` to `Success<A, E>` and access the
* `value` property.
*
* **Example** (Narrowing to success)
*
* ```ts import.meta.vitest
* import { Exit } from "effect"
*
* const exit = Exit.succeed(42)
*
* if (Exit.isSuccess(exit)) {
*   exit.value // => 42
* }
* ```
*
* @see {@link isFailure} for the opposite check
* @see {@link match} for exhaustive pattern matching
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isSuccess = exitIsSuccess;
const DeferredProto = {
	["~effect/Deferred"]: {
		_A: identity,
		_E: identity
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const DeferredImpl = function() {
	this.resumes = void 0;
	this.effect = void 0;
};
DeferredImpl.prototype = DeferredProto;
/**
* Creates an empty `Deferred` synchronously outside the `Effect` runtime.
*
* **When to use**
*
* Use to allocate a `Deferred` synchronously when direct allocation outside
* `Effect` is required.
*
* **Example** (Creating a Deferred unsafely)
*
* ```ts import.meta.vitest
* import { Deferred } from "effect"
*
* const deferred = Deferred.makeUnsafe<number>()
* Deferred.isDoneUnsafe(deferred) // => false
* ```
*
* @stability stable
* @category unsafe
* @since 4.0.0
*/
const makeUnsafe$4 = () => new DeferredImpl();
/**
* Completes the `Deferred` with the specified `Exit` value, which will be
* propagated to all fibers waiting on the value of the `Deferred`.
*
* **When to use**
*
* Use to complete a `Deferred` from an already computed `Exit`.
*
* **Details**
*
* The returned effect succeeds with `true` when this call completed the
* `Deferred`, or `false` if it was already completed.
*
* **Example** (Completing a Deferred with an Exit)
*
* ```ts import.meta.vitest
* import { Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number>()
*   yield* Deferred.done(deferred, Exit.succeed(42))
*   return yield* Effect.exit(Deferred.await(deferred))
* })
*
* await Effect.runPromise(program) // => Exit.succeed(42)
* ```
*
* @see {@link complete} for completing from an effect and memoizing its result
* @see {@link completeWith} for storing an effect directly
* @see {@link succeed} for completing with a success value
* @see {@link failCause} for completing with a failure cause
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const done$1 = /* @__PURE__ */ dual(2, (self, effect) => sync$1(() => doneUnsafe(self, effect)));
/**
* Attempts to complete the `Deferred` with the specified `Cause`.
*
* **When to use**
*
* Use to complete a `Deferred` with a full failure cause.
*
* **Details**
*
* Fibers waiting on the `Deferred` observe that cause only if this call
* completes it. The returned effect succeeds with `true` when this call
* completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Failing a Deferred with a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number, string>()
*   const success = yield* Deferred.failCause(deferred, Cause.fail("Operation failed"))
*   const exit = yield* Effect.exit(Deferred.await(deferred))
*   return [success, exit]
* })
*
* await Effect.runPromise(program) // => [true, Exit.failCause(Cause.fail("Operation failed"))]
* ```
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const failCause$1 = /*#__PURE__*/ dual(2, (self, cause) => done$1(self, exitFailCause(cause)));
/**
* Attempts to complete the `Deferred` with interruption by the specified
* `FiberId`.
*
* **When to use**
*
* Use to complete a `Deferred` as interrupted by a specific fiber id.
*
* **Details**
*
* Fibers waiting on the `Deferred` are interrupted with that fiber id only if
* this call completes it. The returned effect succeeds with `true` when this
* call completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Interrupting a Deferred with a fiber id)
*
* ```ts import.meta.vitest
* import { Deferred, Effect, Exit } from "effect"
*
* const program = Effect.gen(function*() {
*   const deferred = yield* Deferred.make<number>()
*   const success = yield* Deferred.interruptWith(deferred, 42)
*   const exit = yield* Effect.exit(Deferred.await(deferred))
*   return [success, exit]
* })
*
* await Effect.runPromise(program) // => [true, Exit.interrupt(42)]
* ```
*
* @stability stable
* @category completion
* @since 2.0.0
*/
const interruptWith = /*#__PURE__*/ dual(2, (self, fiberId) => failCause$1(self, causeInterrupt(fiberId)));
/**
* Attempts to complete the `Deferred` synchronously with the specified
* completion effect.
*
* **When to use**
*
* Use to complete a `Deferred` synchronously in low-level code that already has
* the completion effect.
*
* **Details**
*
* This mutates the `Deferred` directly and should be reserved for low-level
* code; prefer the effectful completion APIs when possible. Returns `true` if
* this call completed the `Deferred`, or `false` if it was already completed.
*
* **Example** (Completing a Deferred unsafely)
*
* ```ts import.meta.vitest
* import { Deferred, Effect } from "effect"
*
* const deferred = Deferred.makeUnsafe<number>()
* Deferred.doneUnsafe(deferred, Effect.succeed(42)) // => true
* ```
*
* @stability stable
* @category unsafe
* @since 4.0.0
*/
const doneUnsafe = (self, effect) => {
	if (self.effect) return false;
	self.effect = effect;
	if (self.resumes) {
		const resumes = self.resumes;
		self.resumes = void 0;
		for (let i = 0; i < resumes.length; i++) resumes[i](effect);
	}
	return true;
};
// node_modules/effect/dist/Scope.js
/**
* Controls how long resources stay open.
*
* A scope is a lifetime boundary. Code can register cleanup effects on it, and
* closing the scope runs those cleanups with the `Exit` value that ended the
* work. Most application code uses higher-level APIs such as `Effect.scoped`
* and `Layer`, while this module is useful when code needs to create, provide,
* fork, close, or inspect scopes directly.
*
* @stability stable
* @since 2.0.0
*/
/**
* Service tag for the active resource lifetime.
*
* **When to use**
*
* Use to access the active lifetime when registering finalizers or sharing
* resources with the surrounding scope.
*
* **Example** (Accessing the scope service)
*
* ```ts import.meta.vitest
* import { Effect, Scope } from "effect"
*
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.Scope
*   yield* Scope.addFinalizer(scope, Effect.sync(() => cleanups.push("Cleanup")))
* })
*
* Effect.runSync(Effect.scoped(program))
* cleanups // => ["Cleanup"]
* ```
*
* @stability stable
* @category services
* @since 2.0.0
*/
const Scope = scopeTag;
/**
* Creates a new `Scope` synchronously without wrapping it in an `Effect`.
* This is useful when you need a scope immediately but should be used with caution
* as it doesn't provide the same safety guarantees as the `Effect`-wrapped version.
*
* **When to use**
*
* Use when a scope must be allocated synchronously and the caller will close it
* manually.
*
* **Example** (Creating a scope synchronously)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const scope = Scope.makeUnsafe("sequential")
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   yield* Scope.addFinalizer(scope, Effect.sync(() => cleanups.push("Cleanup")))
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(program)
* cleanups // => ["Cleanup"]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe$3 = scopeMakeUnsafe;
/**
* Provides a concrete `Scope` to an effect.
*
* **When to use**
*
* Use to run an effect that requires `Scope` with a scope managed by the
* caller.
*
* **Details**
*
* Providing the scope removes the `Scope` requirement from the effect context.
*
* **Example** (Providing a scope)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.Scope
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup")))
*   events.push("working")
* })
*
* const withScope = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.provide(scope)(program)
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(withScope)
* events // => ["working", "cleanup"]
* ```
*
* @stability stable
* @category combinators
* @since 4.0.0
*/
const provide = provideScope;
/**
* Registers an exit-aware finalizer on a scope.
*
* **When to use**
*
* Use when cleanup needs to know whether the scope closed with success,
* failure, or interruption.
*
* **Details**
*
* If the scope is open, the finalizer runs when the scope closes and receives
* the scope's exit value. If the scope is already closed, the finalizer runs
* immediately with the stored exit value.
*
* **Example** (Adding an exit-aware finalizer)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const exits: Array<Exit.Exit<unknown, unknown>> = []
* const withResource = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.addFinalizerExit(scope, (exit) => Effect.sync(() => exits.push(exit)))
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(withResource)
* exits // => [Exit.void]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const addFinalizerExit = scopeAddFinalizerExit;
/**
* Registers a finalizer effect on a scope.
*
* **Details**
*
* If the scope is open, the finalizer runs when the scope closes, regardless of
* whether the scope closes successfully or with an error. If the scope is
* already closed, the finalizer runs immediately.
*
* **Example** (Adding finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const program = Effect.gen(function*() {
*   const scope = yield* Scope.make()
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 1")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 2")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("cleanup 3")))
*   events.push("work")
*   yield* Scope.close(scope, Exit.void)
* })
*
* Effect.runSync(program)
* events // => ["work", "cleanup 3", "cleanup 2", "cleanup 1"]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const addFinalizer$1 = scopeAddFinalizer;
/**
* Creates a closeable child scope synchronously and registers it with a parent scope.
*
* **When to use**
*
* Use when a child scope must be created synchronously and the caller controls
* both parent and child scope lifetimes.
*
* **Details**
*
* Closing the parent closes the child with the same exit value, and closing the
* child detaches it from the parent. The optional finalizer strategy configures
* the child scope and defaults to `"sequential"` when omitted.
*
* **Example** (Creating a child scope synchronously)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const cleanups: Array<string> = []
* const program = Effect.gen(function*() {
*   const parentScope = Scope.makeUnsafe("sequential")
*   const childScope = Scope.forkUnsafe(parentScope, "parallel")
*   yield* Scope.addFinalizer(parentScope, Effect.sync(() => cleanups.push("parent")))
*   yield* Scope.addFinalizer(childScope, Effect.sync(() => cleanups.push("child")))
*   yield* Scope.close(childScope, Exit.void)
*   yield* Scope.close(parentScope, Exit.void)
* })
*
* Effect.runSync(program)
* cleanups // => ["child", "parent"]
* ```
*
* @stability stable
* @category combinators
* @since 4.0.0
*/
const forkUnsafe = scopeForkUnsafe;
/**
* Closes a scope and runs its registered finalizers.
*
* **When to use**
*
* Use to close a scope manually with a specific exit value.
*
* **Details**
*
* Finalizers run in the scope's configured order and receive the supplied
* `Exit`.
* By default, finalizers run uninterruptibly, so interrupting the closing fiber
* waits for them to finish; a finalizer can explicitly restore interruptibility.
*
* **Gotchas**
*
* Closing a scope that is already closed returns immediately, even when the
* finalizers of an earlier close are still running.
*
* **Example** (Running scope finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit, Scope } from "effect"
*
* const events: Array<string> = []
* const resourceManagement = Effect.gen(function*() {
*   const scope = yield* Scope.make("sequential")
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("database")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("file")))
*   yield* Scope.addFinalizer(scope, Effect.sync(() => events.push("memory")))
*   events.push("work")
*   yield* Scope.close(scope, Exit.succeed("Success!"))
* })
*
* Effect.runSync(resourceManagement)
* events // => ["work", "memory", "file", "database"]
* ```
*
* @stability stable
* @category combinators
* @since 2.0.0
*/
const close = scopeClose;
// node_modules/effect/dist/Cause.js
/**
* Narrows a `Reason` to `Fail`.
*
* **When to use**
*
* Use as a predicate for `Array.filter` to pick out typed `Fail` reasons when
* iterating over `cause.reasons`.
*
* **Example** (Filtering fail reasons)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* const cause = Cause.fail("error")
* const fails = cause.reasons.filter(Cause.isFailReason)
* fails[0].error // => "error"
* ```
*
* @see {@link isDieReason} — narrow to `Die`
* @see {@link isInterruptReason} — narrow to `Interrupt`
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isFailReason = isFailReason$1;
/**
* Creates a `Cause` from an array of `Reason` values.
*
* **When to use**
*
* Use when you already have individual reasons (e.g. from filtering or
* transforming another cause's `reasons` array) and need to wrap them back
* into a `Cause`.
*
* **Details**
*
* - Returns a new `Cause`.
* - An empty array produces a cause equivalent to `empty`.
*
* **Gotchas**
*
* The `reasons` array is stored as provided. Treat the array as immutable
* after passing it to this function.
*
* **Example** (Building a cause from reasons)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* const reasons = [
*   Cause.makeFailReason("err1"),
*   Cause.makeFailReason("err2")
* ]
* Cause.fromReasons(reasons) // => Cause.combine(Cause.fail("err1"), Cause.fail("err2"))
* ```
*
* @see {@link combine} — merge two existing causes
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromReasons = causeFromReasons;
/**
* Creates a `Cause` containing a single `Die` reason with the
* given defect.
*
* **When to use**
*
* Use to construct a cause from an untyped defect or unexpected thrown value.
*
* **Example** (Creating a die cause)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* Cause.die("Unexpected") // => Cause.fromReasons([Cause.makeDieReason("Unexpected")])
* ```
*
* @see {@link fail} — for typed errors
* @see {@link interrupt} — for fiber interruptions
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const die$1 = causeDie;
/**
* Transforms the typed error values inside a `Cause` using the
* provided function. Only `Fail` reasons are affected; `Die` and `Interrupt`
* reasons pass through unchanged.
*
* **When to use**
*
* Use to transform expected typed failures while preserving defects and
* interruptions unchanged.
*
* **Details**
*
* If at least one `Fail` reason exists, this returns a new `Cause`
* containing the mapped failures. If the cause has no `Fail` reasons, the
* original cause is returned unchanged.
*
* **Example** (Mapping errors to uppercase)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* const cause = Cause.fail("error")
* const mapped = Cause.map(cause, (e) => e.toUpperCase())
* const reason = mapped.reasons[0]
* if (Cause.isFailReason(reason)) {
*   reason.error // => "ERROR"
* }
* ```
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map$1 = causeMap;
/**
* Checks whether an arbitrary value is a `Done` signal.
*
* **Example** (Checking the runtime type)
*
* ```ts import.meta.vitest
* import { Cause } from "effect"
*
* Cause.isDone(Cause.Done()) // => true
* Cause.isDone("not done") // => false
* ```
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isDone = isDone$1;
/**
* Creates an Effect that fails with a `Done` error. Shorthand for
* `Effect.fail(Cause.Done(value))`.
*
* **When to use**
*
* Use when you model stream or queue completion through the error channel.
*
* **Example** (Failing with Done)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Exit } from "effect"
*
* const program = Cause.done("finished")
*
* await Effect.runPromiseExit(program) // => Exit.fail(Cause.Done("finished"))
* ```
*
* @see {@link Done} — create the signal value without an Effect
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const done = done$2;
// node_modules/effect/dist/internal/dateTime.js
/** @internal */
const TypeId$18 = "~effect/DateTime";
/** @internal */
const TimeZoneTypeId = "~effect/DateTime/TimeZone";
const Proto$4 = {
	[TypeId$18]: TypeId$18,
	pipe() {
		return pipeArguments(this, arguments);
	},
	[NodeInspectSymbol]() {
		return this.toString();
	},
	toJSON() {
		return toDateUtc$1(this).toJSON();
	}
};
const ProtoUtc = {
	...Proto$4,
	_tag: "Utc",
	[symbol$1]() {
		return number$1(this.epochMilliseconds);
	},
	[symbol](that) {
		return isDateTime$1(that) && that._tag === "Utc" && this.epochMilliseconds === that.epochMilliseconds;
	},
	toString() {
		return `DateTime.Utc(${toDateUtc$1(this).toJSON()})`;
	}
};
const ProtoZoned = {
	...Proto$4,
	_tag: "Zoned",
	[symbol$1]() {
		return combine(number$1(this.epochMilliseconds))(hash(this.zone));
	},
	[symbol](that) {
		return isDateTime$1(that) && that._tag === "Zoned" && this.epochMilliseconds === that.epochMilliseconds && equals$1(this.zone, that.zone);
	},
	toString() {
		return `DateTime.Zoned(${formatIsoZoned(this)})`;
	}
};
const ProtoTimeZone = {
	[TimeZoneTypeId]: TimeZoneTypeId,
	[NodeInspectSymbol]() {
		return this.toString();
	}
};
({ ...ProtoTimeZone });
({ ...ProtoTimeZone });
const cacheSlot = {
	value: void 0,
	enumerable: false,
	writable: true
};
const sealedCaches = /*#__PURE__*/ new WeakMap();
const cacheAccessor = (key, slots) => ({
	get() {
		return Object.isExtensible(this) ? void 0 : sealedCaches.get(this)?.[key];
	},
	set(value) {
		if (Object.isExtensible(this)) {
			for (const slot of slots) Object.defineProperty(this, slot, cacheSlot);
			this[key] = value;
		} else {
			let caches = sealedCaches.get(this);
			if (caches === void 0) {
				caches = {};
				sealedCaches.set(this, caches);
			}
			caches[key] = value;
		}
	}
});
const zonedSlots = [
	"partsUtc",
	"adjustedEpochMilliseconds",
	"partsAdjusted"
];
Object.defineProperty(ProtoUtc, "partsUtc", cacheAccessor("partsUtc", ["partsUtc"]));
Object.defineProperties(ProtoZoned, {
	partsUtc: cacheAccessor("partsUtc", zonedSlots),
	adjustedEpochMilliseconds: cacheAccessor("adjustedEpochMilliseconds", zonedSlots),
	partsAdjusted: cacheAccessor("partsAdjusted", zonedSlots)
});
/** @internal */
const isDateTime$1 = (u) => hasProperty(u, TypeId$18);
/** @internal */
const toDateUtc$1 = (self) => new Date(self.epochMilliseconds);
/** @internal */
const toDate = (self) => {
	if (self._tag === "Utc") return new Date(self.epochMilliseconds);
	else if (self.zone._tag === "Offset") return new Date(self.epochMilliseconds + self.zone.offset);
	else if (self.adjustedEpochMilliseconds !== void 0) return new Date(self.adjustedEpochMilliseconds);
	let bc = false;
	const parts = self.zone.format.formatToParts(self.epochMilliseconds).filter((_) => {
		if (_.type === "era") {
			bc = _.value === "BC";
			return false;
		}
		return _.type !== "literal";
	});
	const year = Number(parts[2].value);
	const date = /* @__PURE__ */ new Date(0);
	date.setUTCFullYear(bc ? 1 - year : year, Number(parts[0].value) - 1, Number(parts[1].value));
	date.setUTCHours(Number(parts[3].value), Number(parts[4].value), Number(parts[5].value), parts[6]?.type === "fractionalSecond" ? Number(parts[6].value) : (self.epochMilliseconds % 1e3 + 1e3) % 1e3);
	self.adjustedEpochMilliseconds = date.getTime();
	return date;
};
/** @internal */
const zonedOffset = (self) => {
	return toDate(self).getTime() - toEpochMillis$1(self);
};
const offsetToString = (offset) => {
	const abs = Math.abs(offset);
	let hours = Math.floor(abs / 36e5);
	let minutes = Math.round(abs % 36e5 / 6e4);
	if (minutes === 60) {
		hours += 1;
		minutes = 0;
	}
	return `${offset < 0 ? "-" : "+"}${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
};
/** @internal */
const zonedOffsetIso = (self) => offsetToString(zonedOffset(self));
/** @internal */
const toEpochMillis$1 = (self) => self.epochMilliseconds;
/** @internal */
const formatIsoOffset = (self) => {
	const date = toDate(self);
	return self._tag === "Utc" ? date.toISOString() : `${date.toISOString().slice(0, -1)}${zonedOffsetIso(self)}`;
};
/** @internal */
const formatIsoZoned = (self) => self.zone._tag === "Offset" ? formatIsoOffset(self) : `${formatIsoOffset(self)}[${self.zone.id}]`;
// node_modules/effect/dist/Pull.js
/**
* Models one low-level pull step for stream-like consumers.
*
* A `Pull<A, E, Done, R>` is an `Effect` that can produce one `A`, fail with an
* ordinary error `E`, or signal end-of-input with `Cause.Done<Done>`. The
* separate done signal lets low-level consumers distinguish normal completion
* from failure. This module includes type extractors and helpers for detecting,
* filtering, catching, converting, and matching done signals separately from
* ordinary failures.
*
* @stability stable
* @since 4.0.0
*/
/**
* Handles `Cause.Done` failures in an effect while leaving ordinary failures
* in the error channel.
*
* **When to use**
*
* Use to recover from a `Cause.Done` completion signal in an effect, such as
* turning a pull leftover value into a successful recovery effect while
* preserving ordinary failures.
*
* **Details**
*
* The handler receives the done leftover value and may recover with a new
* effect. Non-done errors are preserved.
*
* @see {@link matchEffect} for handling success, ordinary failure, and done outcomes explicitly
* @see {@link filterDoneLeftover} for extracting a done leftover from an existing `Cause`
*
* @stability stable
* @category error handling
* @since 4.0.0
*/
const catchDone = /*#__PURE__*/ dual(2, (effect, f) => catchCauseFilter(effect, filterDoneLeftover, (l) => f(l)));
/**
* Checks whether a Cause contains any done errors.
*
* **When to use**
*
* Use when you need to test whether a pull failure cause represents normal
* completion and only need a boolean result.
*
* @see {@link isDoneFailure} for checking a single `Cause.Reason`
* @see {@link filterDone} for extracting the `Cause.Done` value from a `Cause`
* @see {@link filterNoDone} for selecting causes with no done failures
*
* @stability stable
* @category predicates
* @since 4.0.0
*/
const isDoneCause = (cause) => cause.reasons.some(isDoneFailure);
/**
* Checks whether a `Cause.Reason` is a `Fail` reason whose error is a
* `Cause.Done` signal.
*
* **When to use**
*
* Use when you need to identify done completion reasons while traversing
* `cause.reasons`, before handling ordinary failures.
*
* @see {@link isDoneCause} for checking an entire `Cause` for any done reason
* @see {@link filterDone} for extracting the `Cause.Done` value from a `Cause`
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isDoneFailure = (failure) => failure._tag === "Fail" && isDone(failure.error);
/**
* Finds a `Cause.Done` failure in a `Cause`.
*
* **When to use**
*
* Use to separate `Cause.Done` completion from ordinary causes while preserving
* the typed done value.
*
* **Details**
*
* Returns a successful `Result` with the `Cause.Done` value when the cause
* contains a done signal and no other failures besides interruptions. When the
* done signal was merged with a real failure (for example a failing
* finalizer), the `Result` fails with the remaining cause, stripped of the
* done signal. Without a done signal the `Result` fails with the original
* cause.
*
* @stability stable
* @category filtering
* @since 4.0.0
*/
const filterDone = (cause) => {
	let done;
	let hasFailure = false;
	for (const reason of cause.reasons) if (isDoneFailure(reason)) done ??= reason.error;
	else if (reason._tag !== "Interrupt") hasFailure = true;
	if (done === void 0) return fail$4(cause);
	return hasFailure ? fail$4(fromReasons(cause.reasons.filter((reason) => !isDoneFailure(reason)))) : succeed$4(done);
};
/**
* Filters a Cause to extract the leftover value from done errors.
*
* **When to use**
*
* Use to extract only the leftover value carried by a `Cause.Done` completion
* signal.
*
* @stability stable
* @category filtering
* @since 4.0.0
*/
const filterDoneLeftover = (cause) => {
	const done = filterDone(cause);
	return isFailure(done) ? done : succeed$4(done.success.value);
};
/**
* Pattern matches on a Pull, handling success, failure, and done cases.
*
* **When to use**
*
* Use to handle all three `Pull` outcomes with effectful handlers.
*
* **Example** (Matching Pull outcomes)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Pull } from "effect"
*
* const pull = Cause.done("stream ended")
*
* const result = Pull.matchEffect(pull, {
*   onSuccess: (value) => Effect.succeed(`Got value: ${value}`),
*   onFailure: (cause) => Effect.succeed(`Got error: ${cause}`),
*   onDone: (leftover) => Effect.succeed(`Stream halted with: ${leftover}`)
* })
*
* await Effect.runPromise(result) // => "Stream halted with: stream ended"
* ```
*
* @stability stable
* @category pattern matching
* @since 4.0.0
*/
const matchEffect = /*#__PURE__*/ dual(2, (self, options) => matchCauseEffect(self, {
	onSuccess: options.onSuccess,
	onFailure: (cause) => {
		const halt = filterDone(cause);
		return !isFailure(halt) ? options.onDone(halt.success.value) : options.onFailure(halt.failure);
	}
}));
// node_modules/effect/dist/Schedule.js
const TypeId$17 = "~effect/Schedule";
/**
* Context reference containing metadata for the currently running schedule step.
*
* **Details**
*
* Repeat, retry, stream, and channel scheduling operations provide this service
* to effects run between schedule steps. The default value contains undefined
* input and output values, zero duration, and zeroed timing fields before any
* schedule step has produced metadata.
*
* @stability stable
* @category services
* @since 4.0.0
*/
const CurrentMetadata = /*#__PURE__*/ Reference("effect/Schedule/CurrentMetadata", { defaultValue: /*#__PURE__*/ constant({
	input: void 0,
	output: void 0,
	duration: zero$1,
	attempt: 0,
	start: 0,
	now: 0,
	elapsed: 0,
	elapsedSincePrevious: 0
}) });
const ScheduleProto = {
	[TypeId$17]: {
		_Out: identity,
		_In: identity,
		_Env: identity
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/**
* Type guard that checks if a value is a Schedule.
*
* **Example** (Checking for schedules)
*
* ```ts import.meta.vitest
* import { Schedule } from "effect"
*
* const schedule = Schedule.exponential("100 millis")
* const notSchedule = { foo: "bar" }
*
* Schedule.isSchedule(schedule) // => true
* Schedule.isSchedule(notSchedule) // => false
* Schedule.isSchedule(null) // => false
* Schedule.isSchedule(undefined) // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isSchedule = (u) => hasProperty(u, TypeId$17);
/**
* Creates a Schedule from a step function that returns a Pull.
*
* **Example** (Creating a custom schedule from a step function)
*
* ```ts import.meta.vitest
* import { Cause, Duration, Effect, Schedule } from "effect"
*
* const schedule = Schedule.fromStep(Effect.sync(() => {
*   let count = 0
*
*   return (_now: number, _input: string) => {
*     if (count >= 3) {
*       return Cause.done(count)
*     }
*     return Effect.succeed([count++, Duration.millis(100)] as [number, Duration.Duration])
*   }
* }))
*
* const program = Effect.gen(function*() {
*   const step = yield* Schedule.toStep(schedule)
*   const [output] = yield* step(0, "input")
*   return output
* })
*
* await Effect.runPromise(program) // => 0
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromStep = (step) => {
	const self = Object.create(ScheduleProto);
	self.step = step;
	return self;
};
const metadataFn = () => {
	let n = 0;
	let previous;
	let start;
	return (now, input) => {
		if (start === void 0) start = now;
		const elapsed = now - start;
		const elapsedSincePrevious = previous === void 0 ? 0 : now - previous;
		previous = now;
		return {
			input,
			attempt: ++n,
			start,
			now,
			elapsed,
			elapsedSincePrevious
		};
	};
};
/**
* Creates a Schedule from a step function that receives metadata about the schedule's execution.
*
* **Example** (Creating a metadata-aware schedule)
*
* ```ts import.meta.vitest
* import { Cause, Duration, Effect, Schedule } from "effect"
*
* const firstThreeInputs = Schedule.fromStepWithMetadata(Effect.succeed((metadata: Schedule.InputMetadata<string>) => {
*   if (metadata.attempt > 3) {
*     return Cause.done("finished")
*   }
*
*   return Effect.succeed([
*     `attempt ${metadata.attempt}: ${metadata.input}`,
*     Duration.millis(250)
*   ] as [string, Duration.Duration])
* }))
*
* const program = Effect.gen(function*() {
*   const step = yield* Schedule.toStep(firstThreeInputs)
*   const [output] = yield* step(0, "input")
*   return output
* })
*
* await Effect.runPromise(program) // => "attempt 1: input"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromStepWithMetadata = (step) => fromStep(map$2(step, (f) => {
	const meta = metadataFn();
	return (now, input) => f(meta(now, input));
}));
/**
* Extracts the step function from a Schedule.
*
* **Example** (Extracting a schedule step function)
*
* ```ts import.meta.vitest
* import { Duration, Effect, Schedule } from "effect"
*
* // Extract step function from an existing schedule
* const schedule = Schedule.exponential("100 millis").pipe(Schedule.upTo({ times: 3 }))
*
* const program = Effect.gen(function*() {
*   const stepFn = yield* Schedule.toStep(schedule)
*
*   // Use the step function directly for custom logic. The timestamp is
*   // supplied by the caller, so tests can pass a deterministic value.
*   const now = 0
*   return yield* stepFn(now, "input")
* })
*
* await Effect.runPromise(program) // => [Duration.millis(100), Duration.millis(100)]
* ```
*
* @stability stable
* @category destructors
* @since 4.0.0
*/
const toStep = (schedule) => catchCause$1(schedule.step, (cause) => succeed$3(() => failCause$3(cause)));
/**
* Extracts a step function from a `Schedule` that sleeps for each computed
* delay and returns metadata for the completed step.
*
* **When to use**
*
* Use to drive a schedule manually while preserving the computed output,
* delay, input, attempt, and elapsed timing metadata for each step.
*
* **Details**
*
* The returned step reads the current time from `Clock` when invoked, calls the
* schedule step with that timestamp and input, sleeps for the returned
* duration, and then yields `Metadata`.
*
* @see {@link toStep} for manually supplying the timestamp and handling the returned delay yourself
* @see {@link toStepWithSleep} for the same automatic sleeping behavior when only the schedule output is needed
*
* @stability stable
* @category destructors
* @since 4.0.0
*/
const toStepWithMetadata = (schedule) => clockWith((clock) => map$2(toStep(schedule), (step) => {
	const metaFn = metadataFn();
	return (input) => suspend$1(() => {
		const now = clock.currentTimeMillisUnsafe();
		return flatMap$1(step(now, input), ([output, duration]) => {
			const meta = metaFn(now, input);
			meta.output = output;
			meta.duration = duration;
			return as$1(sleep(duration), meta);
		});
	});
}));
/**
* Returns a new `Schedule` that outputs the inputs of the specified schedule.
*
* **Example** (Passing inputs through as outputs)
*
* ```ts import.meta.vitest
* import { Effect, Schedule } from "effect"
*
* const inputSchedule = Schedule.passthrough(
*   Schedule.exponential("100 millis").pipe(Schedule.upTo({ times: 3 }))
* )
* const program = Effect.gen(function*() {
*   const step = yield* Schedule.toStep(inputSchedule)
*   const [output] = yield* step(0, "input")
*   return output
* })
*
* await Effect.runPromise(program) // => "input"
* ```
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const passthrough$2 = (self) => fromStep(map$2(toStep(self), (step) => (now, input) => matchEffect(step(now, input), {
	onSuccess: (result) => succeed$3([input, result[1]]),
	onFailure: failCause$3,
	onDone: () => done(input)
})));
/**
* Returns a schedule that recurs continuously, each repetition spaced the
* specified duration from the last run.
*
* **When to use**
*
* Use when each delay should start after the previous action completes.
*
* **Example** (Repeating with fixed spacing)
*
* ```ts import.meta.vitest
* import { Duration, Effect, Schedule } from "effect"
*
* const program = Effect.gen(function*() {
*   const step = yield* Schedule.toStep(Schedule.spaced("2 seconds"))
*   return yield* step(0, undefined)
* })
*
* await Effect.runPromise(program) // => [0, Duration.seconds(2)]
* ```
*
* @see {@link fixed} for recurrence aligned to a regular cadence
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const spaced = (duration) => {
	const decoded = fromInputUnsafe(duration);
	return fromStepWithMetadata(succeed$3((meta) => succeed$3([meta.attempt - 1, decoded])));
};
const while_ = /*#__PURE__*/ dual(2, (self, predicate) => fromStep(map$2(toStep(self), (step) => {
	const meta = metadataFn();
	return (now, input) => flatMap$1(step(now, input), (result) => {
		const [output, duration] = result;
		const eff = predicate({
			...meta(now, input),
			output,
			duration
		});
		return flatMap$1(isEffect(eff) ? eff : succeed$3(eff), (check) => check ? succeed$3(result) : done(output));
	});
})));
/**
* Returns a new `Schedule` that will recur forever.
*
* **Details**
*
* The output of the schedule is the current count of its repetitions thus far
* (i.e. `0, 1, 2, ...`).
*
* **Example** (Repeating forever)
*
* ```ts import.meta.vitest
* import { Effect, Schedule } from "effect"
* import { TestClock } from "effect/testing"
*
* const executions: Array<number> = []
* const schedule = Schedule.forever.pipe(Schedule.upTo({ times: 2 }))
* const program = Effect.sync(() => executions.push(executions.length + 1)).pipe(
*   Effect.repeat(schedule),
*   Effect.as(executions)
* )
*
* await Effect.runPromise(Effect.provide(program, TestClock.layer())) // => [1, 2, 3]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const forever$1 = /*#__PURE__*/ spaced(zero$1);
// node_modules/effect/dist/internal/schedule.js
/** @internal */
const repeatOrElse = /*#__PURE__*/ dual(3, (self, schedule, orElse) => flatMap$1(toStepWithMetadata(schedule), (step) => {
	let meta = CurrentMetadata.defaultValue();
	const loop = (input) => matchEffect(step(input), {
		onSuccess(meta_) {
			meta = meta_;
			return flatMap$1(provideService(self, CurrentMetadata, meta), loop);
		},
		onFailure: failCause$3,
		onDone: succeed$3
	});
	return catchCauseFilter(flatMap$1(provideService(self, CurrentMetadata, meta), loop), findRecoverableError, (error) => orElse(error, meta.attempt === 0 ? none() : some(meta)));
}));
/** @internal */
const repeat$1 = /*#__PURE__*/ dual(2, (self, options) => {
	const schedule = typeof options === "function" ? options(identity) : isSchedule(options) ? options : buildFromOptions(options);
	return repeatOrElse(self, schedule, fail$3);
});
const passthroughForever = /*#__PURE__*/ passthrough$2(forever$1);
/** @internal */
const buildFromOptions = (options) => {
	let schedule = options.schedule ? passthrough$2(options.schedule) : passthroughForever;
	if (options.while) schedule = while_(schedule, ({ input }) => {
		const applied = options.while(input);
		return isEffect(applied) ? applied : succeed$3(applied);
	});
	if (options.until) schedule = while_(schedule, ({ input }) => {
		const applied = options.until(input);
		return isEffect(applied) ? map$2(applied, (b) => !b) : succeed$3(!applied);
	});
	if (options.times !== void 0) schedule = while_(schedule, ({ attempt }) => succeed$3(attempt <= options.times));
	return schedule;
};
// node_modules/effect/dist/Effect.js
/**
* Executes an effectful operation for each element in an `Iterable`.
*
* **When to use**
*
* Use to traverse an iterable with an effectful function while preserving
* element order in the collected results.
*
* **Details**
*
* The `forEach` function applies a provided operation to each element in the
* iterable, producing a new effect that returns an array of results.
*
* If any effect fails, the iteration stops immediately (short-circuiting), and
* the error is propagated.
*
* Concurrency:
*
* The `concurrency` option controls how many operations are performed
* concurrently. By default, the operations are performed sequentially.
*
* Discarding Results:
*
* If the `discard` option is set to `true`, the intermediate results are not
* collected, and the final result of the operation is `void`.
*
* **Example** (Mapping over an iterable with effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const result = Effect.forEach(
*   [1, 2, 3, 4, 5],
*   (n, index) =>
*     Effect.sync(() => { output.push(`Currently at index ${index}`) }).pipe(Effect.as(n * 2))
* )
*
* void output.push(await Effect.runPromise(result))
* output // => ["Currently at index 0", "Currently at index 1", "Currently at index 2", "Currently at index 3", "Currently at index 4", [2, 4, 6, 8, 10]]
* ```
*
* **Example** (Running effects without collecting results)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* // Apply effects but discard the results
* const result = Effect.forEach(
*   [1, 2, 3, 4, 5],
*   (n, index) =>
*     Effect.sync(() => { output.push(`Currently at index ${index}`) }).pipe(Effect.as(n * 2)),
*   { discard: true }
* )
*
* void output.push(await Effect.runPromise(result))
* output // => ["Currently at index 0", "Currently at index 1", "Currently at index 2", "Currently at index 3", "Currently at index 4", undefined]
* ```
*
* @see {@link all} for combining multiple effects into one.
* @stability stable
* @category collecting
* @since 2.0.0
*/
const forEach = forEach$1;
/**
* Executes a body effect repeatedly while a condition holds true.
*
* **Example** (Repeating an effectful loop)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* let counter = 0
*
* const program = Effect.whileLoop({
*   while: () => counter < 5,
*   body: () => Effect.sync(() => ++counter),
*   step: (n) => void output.push(`Current count: ${n}`)
* })
*
* await Effect.runPromise(program)
* output // => ["Current count: 1", "Current count: 2", "Current count: 3", "Current count: 4", "Current count: 5"]
* ```
*
* @stability stable
* @category repetition
* @since 2.0.0
*/
const whileLoop = whileLoop$1;
/**
* Creates an `Effect` from an asynchronous computation that may throw or
* reject, mapping failures into the error channel.
*
* **When to use**
*
* Use when you need to perform asynchronous operations that might fail, such
* as fetching data from an API, and want thrown exceptions or rejected promises
* captured as Effect errors.
*
* **Details**
*
* The promise thunk is evaluated when the effect runs. If it returns a promise
* that resolves, the resolved value becomes the success value. If the thunk
* throws before returning a promise, or if the returned promise rejects, the
* thrown or rejected value is mapped into the error channel.
*
* Passing the thunk directly maps failures to {@link Cause.UnknownError}.
* Passing `{ try, catch }` uses `catch` to map failures to an error of type
* `E`.
*
* If the thunk declares a parameter, it receives an `AbortSignal` that is
* aborted if the effect is interrupted. The underlying asynchronous operation
* only stops if it observes that signal. Zero-argument thunks do not allocate
* an `AbortController`.
*
* **Gotchas**
*
* If `catch` throws while mapping the error, that thrown value is treated as a
* defect. Return the error value you want in the error channel instead of
* throwing it.
*
* The parameter check reads the thunk's `length` property, which does not
* count a rest parameter or any parameter from the first default value
* onward. `(...args) => ...` and `(signal = fallback) => ...` therefore
* receive `undefined` instead of a signal, as do wrappers that forward
* arguments through rest parameters. Declare the signal as an ordinary
* parameter: `(signal) => ...`.
*
* **Example** (Wrapping a fetch request that may fail)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const getTodo = (id: number) =>
*   Effect.tryPromise(() => Promise.resolve({ id, completed: false }))
*
* //      ┌─── Effect<{ id: number; completed: boolean }, UnknownError, never>
* //      ▼
* const program = getTodo(1)
* await Effect.runPromise(program) // => { id: 1, completed: false }
* ```
*
* **Example** (Mapping Promise rejections to a tagged error)
*
* ```ts import.meta.vitest
* import { Data, Effect } from "effect"
*
* class TodoFetchError extends Data.TaggedError("TodoFetchError")<{ readonly cause: unknown }> {}
*
* const getTodo = (id: number) =>
*   Effect.tryPromise({
*     try: () => Promise.reject(`Todo ${id} is unavailable`),
*     // remap the error
*     catch: (cause) => new TodoFetchError({ cause })
*   })
*
* //      ┌─── Effect<never, TodoFetchError, never>
* //      ▼
* const program = Effect.flip(getTodo(1))
* const error = await Effect.runPromise(program)
* error._tag // => "TodoFetchError"
* ```
*
* @see {@link promise} if the effectful computation is asynchronous and does not throw errors.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const tryPromise = tryPromise$1;
/**
* Creates an `Effect` that always succeeds with a given value.
*
* **When to use**
*
* Use when an effect should complete successfully with a specific value without any errors
* or external dependencies.
*
* **Example** (Creating a successful effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // Creating an effect that represents a successful scenario
* //
* //      ┌─── Effect<number, never, never>
* //      ▼
* const success = Effect.succeed(42)
* Effect.runSync(success) // => 42
* ```
*
* @see {@link fail} to create an effect that represents a failure.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const succeed$1 = succeed$3;
/**
* Creates an `Effect` lazily, delaying construction until it is needed.
*
* **When to use**
*
* Use when you need to defer the evaluation of an effect until it is required.
*
* **Details**
*
* `suspend` takes a thunk that represents an effect and delays creating it
* until the suspended effect is evaluated. This is useful for optimizing
* expensive computations, managing circular dependencies such as recursive
* functions, and helping TypeScript unify return types when branches construct
* different effects. Any side effects or scoped captures inside the thunk are
* re-executed on each invocation.
*
* **Example** (Lazily evaluating side effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* let i = 0
*
* const bad = Effect.succeed(i++)
*
* const good = Effect.suspend(() => Effect.succeed(i++))
*
* Effect.runSync(bad) // => 0
* Effect.runSync(bad) // => 0
*
* Effect.runSync(good) // => 1
* Effect.runSync(good) // => 2
* ```
*
* **Example** (Suspending recursive Fibonacci evaluation)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const blowsUp = (n: number): Effect.Effect<number> =>
*   n < 2
*     ? Effect.succeed(1)
*     : Effect.zipWith(blowsUp(n - 1), blowsUp(n - 2), (a, b) => a + b)
*
* // console.log(Effect.runSync(blowsUp(32)))
* // crash: JavaScript heap out of memory
*
* const allGood = (n: number): Effect.Effect<number> =>
*   n < 2
*     ? Effect.succeed(1)
*     : Effect.zipWith(
*         Effect.suspend(() => allGood(n - 1)),
*         Effect.suspend(() => allGood(n - 2)),
*         (a, b) => a + b
*       )
*
* Effect.runSync(allGood(16)) // => 1597
* ```
*
* **Example** (Helping TypeScript infer recursive effect types)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* //   Without suspend, TypeScript may struggle with type inference.
* //   Inferred type:
* //     (a: number, b: number) =>
* //       Effect<never, Error, never> | Effect<number, never, never>
* const withoutSuspend = (a: number, b: number) =>
*   b === 0
*     ? Effect.fail(new Error("Cannot divide by zero"))
*     : Effect.succeed(a / b)
*
* //   Using suspend to unify return types.
* //   Inferred type:
* //     (a: number, b: number) => Effect<number, Error, never>
* const withSuspend = (a: number, b: number) =>
*   Effect.suspend(() =>
*     b === 0
*       ? Effect.fail(new Error("Cannot divide by zero"))
*       : Effect.succeed(a / b)
*   )
*
* Effect.runSync(withSuspend(6, 2)) // => 3
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const suspend = suspend$1;
/**
* Creates an `Effect` that represents a synchronous side-effectful computation.
*
* **When to use**
*
* Use when you need to wrap a synchronous side-effectful operation that is not
* expected to throw.
*
* **Details**
*
* The provided function is evaluated lazily when the effect runs.
*
* **Gotchas**
*
* The function must not throw. If it throws, the thrown value is treated as a
* defect, not as a typed failure. Use `try` when throwing is expected.
*
* **Example** (Capturing synchronous logging in an Effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const log = (message: string) =>
*   Effect.sync(() => {
*     void output.push(message) // side effect
*   })
*
* //      ┌─── Effect<void, never, never>
* //      ▼
* const program = log("Hello, World!")
* Effect.runSync(program)
* output // => ["Hello, World!"]
* ```
*
* @see {@link try_ | try} for a version that can handle failures.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const sync = sync$1;
const void_ = void_$2;
/**
* Creates an `Effect` from a callback-based asynchronous API.
*
* **When to use**
*
* Use when you need to integrate APIs that complete through callbacks instead
* of returning a `Promise`.
*
* **Details**
*
* The registration function receives a `resume` callback and, if it declares
* a second parameter, an `AbortSignal` that is aborted if the fiber is
* interrupted. Call `resume` at most once with the effect that should complete
* the fiber; later calls are ignored. Return an optional cleanup effect from
* the registration function to run if the fiber is interrupted.
*
* **Gotchas**
*
* The parameter check reads the registration function's `length` property,
* which must be at least `2` and does not count a rest parameter or any
* parameter from the first default value onward. `(...args) => ...` and
* `(resume, signal = fallback) => ...` therefore receive `undefined` instead
* of a signal, as do wrappers that forward arguments through rest parameters.
* Declare both as ordinary parameters: `(resume, signal) => ...`.
*
* **Example** (Integrating callback APIs)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const fromCallback = (message: string) =>
*   Effect.callback<void>((resume) => {
*     queueMicrotask(() => {
*       void output.push(message)
*       resume(Effect.void)
*     })
*   })
*
* await Effect.runPromise(fromCallback("callback completed"))
* output // => ["callback completed"]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const callback = callback$1;
/**
* Provides a way to write effectful code using generator functions, simplifying
* control flow and error handling.
*
* **When to use**
*
* Use when you want to write effectful code that looks and behaves like
* synchronous code, while still handling asynchronous tasks, errors, and complex
* control flow such as loops and conditions.
*
* Generator functions work similarly to `async/await` but keep errors,
* requirements, and interruption in the Effect type. You can `yield*` values
* from effects and return the final result at the end.
*
* **Example** (Sequencing effects with generators)
*
* ```ts import.meta.vitest
* import { Data, Effect } from "effect"
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* const addServiceCharge = (amount: number) => amount + 1
*
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* const fetchDiscountRate = Effect.promise(() => Promise.resolve(5))
*
* export const program = Effect.gen(function*() {
*   const transactionAmount = yield* fetchTransactionAmount
*   const discountRate = yield* fetchDiscountRate
*   const discountedAmount = yield* applyDiscount(
*     transactionAmount,
*     discountRate
*   )
*   const finalAmount = addServiceCharge(discountedAmount)
*   return `Final amount to charge: ${finalAmount}`
* })
*
* await Effect.runPromise(program) // => "Final amount to charge: 96"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const gen = gen$1;
/**
* Creates an `Effect` that represents a recoverable error.
*
* **When to use**
*
* Use to explicitly signal a recoverable error in an `Effect`.
*
* **Details**
*
* The error keeps propagating unless it is handled. You can handle tagged
* errors with functions like {@link catchTag} or {@link catchTags}.
*
* **Example** (Creating a failed effect)
*
* ```ts import.meta.vitest
* import { Data, Effect } from "effect"
*
* class OperationFailedError extends Data.TaggedError("OperationFailedError")<{}> {}
*
* //      ┌─── Effect<never, OperationFailedError, never>
* //      ▼
* const failure = Effect.fail(
*   new OperationFailedError()
* )
* Effect.runSync(Effect.flip(failure))._tag // => "OperationFailedError"
* ```
*
* @see {@link succeed} to create an effect that represents a successful value.
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fail$1 = fail$3;
/**
* Creates an `Effect` that represents a failure with a specific `Cause`.
*
* **When to use**
*
* Use when you already have a full `Cause` and need to preserve defects,
* interruptions, annotations, or combined failures in the effect's failure
* channel.
*
* **Details**
*
* This function allows you to create effects that fail with complex error
* structures, including multiple errors, defects, interruptions, and more.
*
* **Example** (Failing with a full Cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
*
* const program = Effect.failCause(
*   Cause.fail("Network error")
* )
*
* Effect.runSync(Effect.flip(program)) // => "Network error"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCause = failCause$3;
/**
* Creates an `Effect` that represents a failure with a `Cause` computed lazily.
*
* **When to use**
*
* Use to defer computing a full `Cause` until the effect is run.
*
* **Details**
*
* The cause-producing function is evaluated each time the effect is executed.
*
* **Example** (Lazily creating a Cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
*
* const program = Effect.failCauseSync(() =>
*   Cause.fail("Error computed at runtime")
* )
*
* Effect.runSync(Effect.flip(program)) // => "Error computed at runtime"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const failCauseSync = failCauseSync$1;
/**
* Creates an effect that terminates a fiber with a specified error.
*
* **When to use**
*
* Use when you need an `Effect` to report an unrecoverable defect instead of a
* typed error.
*
* **Details**
*
* The `die` function is used to signal a defect, which represents a critical
* and unexpected error in the code. When invoked, it produces an effect that
* does not handle the error and instead terminates the fiber.
*
* The error channel of the resulting effect is of type `never`, indicating that
* it cannot recover from this failure.
*
* **Example** (Failing on division by zero)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* const defect = new Error("Cannot divide by zero")
* const divide = (a: number, b: number) =>
*   b === 0
*     ? Effect.die(defect)
*     : Effect.succeed(a / b)
*
* //      ┌─── Effect<number, never, never>
* //      ▼
* const program = divide(1, 0)
*
* Effect.runSyncExit(program) // => Exit.die(defect)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const die = die$2;
/**
* Provides access to the current fiber within an effect computation.
*
* **Example** (Reading the current fiber)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.withFiber((fiber) => Effect.succeed(typeof fiber.id))
*
* Effect.runSync(program) // => "number"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const withFiber = withFiber$1;
/**
* Chains effects to produce new `Effect` instances, useful for combining
* operations that depend on previous results.
*
* **When to use**
*
* Use when you need to chain multiple effects, ensuring that each
* step produces a new `Effect` while flattening any nested effects that may
* occur.
*
* **Details**
*
* `flatMap` lets you sequence effects so that the result of one effect can be
* used in the next step. It is similar to `flatMap` used with arrays but works
* specifically with `Effect` instances, allowing you to avoid deeply nested
* effect structures.
*
* Since effects are immutable, `flatMap` always returns a new effect instead of
* changing the original one.
*
* **Example** (Choosing flatMap syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const transformation = (n: number) => Effect.succeed(n + 1)
*
* const flatMappedWithPipe = pipe(myEffect, Effect.flatMap(transformation))
* const flatMappedWithDataFirst = Effect.flatMap(myEffect, transformation)
* const flatMappedWithMethod = myEffect.pipe(Effect.flatMap(transformation))
*
* void output.push(Effect.runSync(Effect.all([
*   flatMappedWithPipe,
*   flatMappedWithDataFirst,
*   flatMappedWithMethod
* ])))
* output // => [[2, 2, 2]]
* ```
*
* **Example** (Sequencing dependent effects)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* // Chaining the fetch and discount application using `flatMap`
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(finalAmount) // => 95
* ```
*
* @see {@link tap} for a version that ignores the result of the effect.
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const flatMap = flatMap$1;
/**
* Runs this effect and then runs another effect, optionally using the first
* effect's success value to choose the next effect.
*
* **When to use**
*
* Use when you need one effect to run after another and the second effect may
* depend on the first effect's success value.
*
* **Details**
*
* When the second argument is an `Effect`, the first success value is discarded
* and the returned effect produces the second effect's value. When the second
* argument is a function, it receives the first success value and must return
* the next `Effect`.
*
* Failures or requirements from either effect are preserved in the returned
* effect.
*
* **Example** (Choosing andThen syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const anotherEffect = Effect.succeed("done")
*
* const transformedWithPipe = pipe(myEffect, Effect.andThen(anotherEffect))
* const transformedWithDataFirst = Effect.andThen(myEffect, anotherEffect)
* const transformedWithMethod = myEffect.pipe(Effect.andThen(anotherEffect))
*
* void output.push(Effect.runSync(Effect.all([
*   transformedWithPipe,
*   transformedWithDataFirst,
*   transformedWithMethod
* ])))
* output // => [['done', 'done', 'done']]
* ```
*
* **Example** (Sequencing a discount calculation after fetching a total)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* // Using Effect.map and Effect.flatMap
* const result1 = pipe(
*   fetchTransactionAmount,
*   Effect.map((amount) => amount * 2),
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(result1) // => 190
*
* // Using Effect.andThen
* const result2 = pipe(
*   fetchTransactionAmount,
*   Effect.andThen((amount) => Effect.succeed(amount * 2)),
*   Effect.andThen((amount) => applyDiscount(amount, 5))
* )
*
* await Effect.runPromise(result2) // => 190
* ```
*
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const andThen = andThen$1;
/**
* Runs a side effect with the result of an effect without changing the original
* value.
*
* **When to use**
*
* Use when you need to run an effectful observation, such as logging or
* tracking, while passing the original success value to the next step.
*
* **Details**
*
* `tap` works similarly to `flatMap`, but it ignores the result of the function
* passed to it. The value from the previous effect remains available for the
* next part of the chain. Note that if the side effect fails, the entire chain
* will fail too.
*
* **Example** (Logging a step in a pipeline)
*
* ```ts import.meta.vitest
* import { Data, Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* class DiscountRateError extends Data.TaggedError("DiscountRateError")<{}> {}
*
* // Function to apply a discount safely to a transaction amount
* const applyDiscount = (
*   total: number,
*   discountRate: number
* ): Effect.Effect<number, DiscountRateError> =>
*   discountRate === 0
*     ? Effect.fail(new DiscountRateError())
*     : Effect.succeed(total - (total * discountRate) / 100)
*
* // Simulated asynchronous task to fetch a transaction amount from database
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   // Log the fetched transaction amount
*   Effect.tap((amount) => Effect.sync(() => { output.push(`Apply a discount to: ${amount}`) })),
*   // `amount` is still available!
*   Effect.flatMap((amount) => applyDiscount(amount, 5))
* )
*
* void output.push(await Effect.runPromise(finalAmount))
* output // => ["Apply a discount to: 100", 95]
* ```
*
* @stability stable
* @category sequencing
* @since 2.0.0
*/
const tap = tap$1;
/**
* Transforms an effect to encapsulate both failure and success using the `Exit`
* data type.
*
* **When to use**
*
* Use when you need to inspect the full outcome, including typed failures, defects,
* and interruptions.
*
* **Details**
*
* `exit` wraps an effect's success or failure inside an `Exit` type, allowing
* you to handle both cases explicitly.
*
* The resulting effect cannot fail because the failure is encapsulated within
* the `Exit.Failure` type. The error type is set to `never`, indicating that
* the effect is structured to never fail directly.
*
* **Example** (Capturing completion as Exit)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* const success = Effect.succeed(42)
* const failure = Effect.fail("Something went wrong")
*
* const program1 = Effect.exit(success)
* const program2 = Effect.exit(failure)
*
* Effect.runSync(program1) // => Exit.succeed(42)
*
* Effect.runSync(program2) // => Exit.fail("Something went wrong")
* ```
*
* @see {@link option} for a version that uses `Option` instead.
* @see {@link result} for a version that uses `Result` instead.
*
* @stability stable
* @category error handling
* @since 2.0.0
*/
const exit = exit$1;
/**
* Transforms the value inside an effect by applying a function to it.
*
* **When to use**
*
* Use to transform an effect's success value with a function that returns a
* plain value, producing a new effect without changing the original effect's
* typed error or context requirements.
*
* **Details**
*
* `map` takes a function and applies it to the value contained within an
* effect, creating a new effect with the transformed value.
*
* It's important to note that effects are immutable, meaning that the original
* effect is not modified. Instead, a new effect is returned with the updated
* value.
*
* **Example** (Choosing map syntax variants)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
* const output: Array<unknown> = []
*
* const myEffect = Effect.succeed(1)
* const transformation = (n: number) => n + 1
*
* const mappedWithPipe = pipe(myEffect, Effect.map(transformation))
* const mappedWithDataFirst = Effect.map(myEffect, transformation)
* const mappedWithMethod = myEffect.pipe(Effect.map(transformation))
*
* void output.push(Effect.runSync(Effect.all([
*   mappedWithPipe,
*   mappedWithDataFirst,
*   mappedWithMethod
* ])))
* output // => [[2, 2, 2]]
* ```
*
* **Example** (Adding a service charge)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
*
* const addServiceCharge = (amount: number) => amount + 1
*
* const fetchTransactionAmount = Effect.promise(() => Promise.resolve(100))
*
* const finalAmount = pipe(
*   fetchTransactionAmount,
*   Effect.map(addServiceCharge)
* )
*
* await Effect.runPromise(finalAmount) // => 101
* ```
*
* @see {@link mapError} for a version that operates on the error channel.
* @see {@link mapBoth} for a version that operates on both channels.
* @see {@link flatMap} or {@link andThen} for a version that can return a new effect.
* @stability stable
* @category mapping
* @since 2.0.0
*/
const map = map$2;
/**
* Replaces the value inside an effect with a constant value.
*
* **When to use**
*
* Use to replace a successful value with a constant while preserving failures
* and requirements.
*
* **Details**
*
* `as` allows you to ignore the original value inside an effect and
* replace it with a new constant value.
*
* **Example** (Replacing a success value)
*
* ```ts import.meta.vitest
* import { Effect, pipe } from "effect"
*
* // Replaces the value 5 with the constant "new value"
* const program = pipe(Effect.succeed(5), Effect.as("new value"))
*
* Effect.runSync(program) // => "new value"
* ```
*
* @see {@link map} for deriving the replacement value from the success value
* @see {@link asVoid} for replacing the success value with `void`
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const as = as$1;
/**
* Maps the success value of an `Effect` to `void`, preserving failures.
*
* **Example** (Discarding success values)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.asVoid(Effect.succeed(42))
*
* Effect.runSync(program) // => undefined
* ```
*
* @stability stable
* @category mapping
* @since 2.0.0
*/
const asVoid = asVoid$1;
/**
* Handles both recoverable and unrecoverable errors by providing a recovery
* effect.
*
* **When to use**
*
* Use when you need to recover from an `Effect` by inspecting the full `Cause`,
* including recoverable failures, defects, and interruptions, instead of only
* the typed error value.
*
* **Details**
*
* When to Recover from Defects:
*
* Defects are unexpected errors that typically shouldn't be recovered from, as
* they often indicate serious issues. However, in some cases, such as
* dynamically loaded plugins, controlled recovery might be needed.
*
* **Example** (Recovering from full failure causes)
*
* ```ts import.meta.vitest
* import { Cause, Effect } from "effect"
* const output: Array<unknown> = []
*
* // An effect that might fail in different ways
* const program = Effect.die("Something went wrong")
*
* // Recover from any cause (including defects)
* const recovered = Effect.catchCause(program, (cause) => {
*   if (Cause.hasDies(cause)) {
*     return Effect.sync(() => { output.push("Caught defect") }).pipe(
*       Effect.as("Recovered from defect")
*     )
*   }
*   return Effect.succeed("Unknown error")
* })
*
* void output.push(Effect.runSync(recovered))
* output // => ["Caught defect", "Recovered from defect"]
* ```
*
* @stability stable
* @category error handling
* @since 4.0.0
*/
const catchCause = catchCause$1;
/**
* Discards both the success and failure values of an effect.
*
* **When to use**
*
* Use when an effect should run for its side effects while both success and
* failure values are discarded.
*
* **Details**
*
* Use the `log` option to emit the full {@link Cause} when the effect fails,
* and `message` to prepend a custom log message.
* Set `log: "Unhandled"` to read `References.UnhandledLogLevel` when the
* failure occurs; `undefined` disables logging. Minimum log level filtering
* still applies.
*
* **Gotchas**
*
* Defects take precedence over typed errors. If the cause also contains a
* defect, the failure is not ignored and is propagated with every typed error
* converted to a defect. An interruption alongside a typed error does not
* prevent the error from being ignored.
*
* **Example** (Discarding success and failure values)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* //      ┌─── Effect<number, string, never>
* //      ▼
* const task = Effect.fail("Uh oh!").pipe(Effect.as(5))
*
* //      ┌─── Effect<void, never, never>
* //      ▼
* const program = task.pipe(Effect.ignore)
* Effect.runSync(program) // => undefined
* ```
*
* **Example** (Logging failures while ignoring results)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const task = Effect.fail("Uh oh!")
*
* const program = task.pipe(Effect.ignore)
* Effect.runSync(program) // => undefined
* ```
*
* @stability stable
* @category error handling
* @since 2.0.0
*/
const ignore = ignore$1;
/**
* Runs an effect conditionally based on the result of an effectful boolean
* condition.
*
* **When to use**
*
* Use when you need an effectful check to decide whether another effect should
* run while representing the skipped case explicitly.
*
* **Details**
*
* The condition effect is evaluated first. If it succeeds with `true`, the
* source effect is run and its success value is wrapped in `Option.some`. If it
* succeeds with `false`, the source effect is skipped and the result is
* `Option.none`. If the condition effect fails, that failure is preserved.
*
* **Example** (Conditionally running an effect)
*
* ```ts import.meta.vitest
* import { Effect, Option } from "effect"
* const output: Array<unknown> = []
*
* const shouldLog = true
*
* const program = Effect.when(
*   Effect.sync(() => { output.push("Condition is true!") }),
*   Effect.succeed(shouldLog)
* )
*
* void output.push(Effect.runSync(program))
* output // => ["Condition is true!", Option.some(undefined)]
* ```
*
* @stability stable
* @category filtering
* @since 2.0.0
*/
const when = when$1;
/**
* Transforms the current context using the provided function.
*
* **When to use**
*
* Use to derive an effect from the complete `Context`.
*
* **Details**
*
* This function allows you to access the complete context and perform
* computations based on all available services. This is useful when you need
* to conditionally execute logic based on what services are available.
*
* **Example** (Deriving values from the context)
*
* ```ts import.meta.vitest
* import { Context, Effect, Option } from "effect"
* const output: Array<unknown> = []
*
* const Logger = Context.Service<{
*   log: (msg: string) => void
* }>("Logger")
* const Cache = Context.Service<{
*   get: (key: string) => string | null
* }>("Cache")
*
* const program = Effect.contextWith((services: Context.Context<Context.Service.Identifier<typeof Cache>>) => {
*   const cacheOption = Context.getOption(services, Cache)
*   const hasCache = Option.isSome(cacheOption)
*
*   if (hasCache) {
*     return Effect.gen(function*() {
*       const cache = yield* Effect.service(Cache)
*       yield* Effect.sync(() => { output.push("Using cached data") })
*       return cache.get("user:123") || "default"
*     })
*   } else {
*     return Effect.gen(function*() {
*       yield* Effect.sync(() => { output.push("No cache available, using fallback") })
*       return "fallback data"
*     })
*   }
* })
*
* const withCache = Effect.provideService(program, Cache, {
*   get: () => "cached_value"
* })
* void output.push(Effect.runSync(withCache))
* output // => ["Using cached data", "cached_value"]
* ```
*
* @see {@link context} for reading the complete context as a value
* @see {@link service} for reading one service from the context
*
* @stability stable
* @category accessors
* @since 2.0.0
*/
const contextWith = contextWith$1;
/**
* Constructs a scoped resource from an acquisition effect and a release
* finalizer.
*
* **When to use**
*
* Use to acquire a scoped resource with an explicit release finalizer.
*
* **Details**
*
* If acquisition succeeds, the release finalizer is added to the current scope
* and is guaranteed to run when that scope closes. The finalizer receives the
* `Exit` value used to close the scope.
*
* By default, acquisition is protected by an uninterruptible region. Pass
* `{ interruptible: true }` to allow the acquisition effect to be interrupted.
*
* **Example** (Acquiring and releasing a resource)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
* const output: Array<unknown> = []
*
* // Simulate a resource that needs cleanup
* interface FileHandle {
*   readonly path: string
*   readonly content: string
* }
*
* // Acquire a file handle
* const acquire = Effect.gen(function*() {
*   yield* Effect.sync(() => { output.push("Opening file") })
*   return { path: "/tmp/file.txt", content: "file content" }
* })
*
* // Release the file handle
* const release = (handle: FileHandle, exit: Exit.Exit<unknown, unknown>) =>
*   Effect.sync(() => { output.push(
*     `Closing file ${handle.path} with exit: ${
*       Exit.isSuccess(exit) ? "success" : "failure"
*     }`
*   ) })
*
* // Create a scoped resource
* const resource = Effect.acquireRelease(acquire, release)
*
* // Use the resource within a scope
* const program = Effect.scoped(
*   Effect.gen(function*() {
*     const handle = yield* resource
*     yield* Effect.sync(() => { output.push(`Using file: ${handle.path}`) })
*     return handle.content
*   })
* )
*
* void output.push(Effect.runSync(program))
* output // => ["Opening file", "Using file: /tmp/file.txt", "Closing file /tmp/file.txt with exit: success", "file content"]
* ```
*
* @see {@link acquireDisposable} for resources that implement JavaScript disposal protocols
* @see {@link acquireUseRelease} for bracketing acquire, use, and release in one effect
*
* @stability stable
* @category resource management
* @since 2.0.0
*/
const acquireRelease = acquireRelease$1;
/**
* Adds a finalizer to the current scope.
*
* **When to use**
*
* Use to register low-level cleanup in the current scope.
*
* **Details**
*
* The finalizer runs when the surrounding scope is closed and receives the
* `Exit` value used to close the scope.
*
* **Example** (Registering scope finalizers)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
* const output: Array<unknown> = []
*
* const program = Effect.scoped(
*   Effect.gen(function*() {
*     // Add a finalizer that runs when the scope closes
*     yield* Effect.addFinalizer((exit) =>
*       Effect.sync(() => { output.push(
*         Exit.isSuccess(exit)
*           ? "Cleanup: Operation completed successfully"
*           : "Cleanup: Operation failed, cleaning up resources"
*       ) })
*     )
*
*     yield* Effect.sync(() => { output.push("Performing main operation...") })
*
*     // This could succeed or fail
*     return "operation result"
*   })
* )
*
* void output.push(Effect.runSync(program))
* output // => ["Performing main operation...", "Cleanup: Operation completed successfully", "operation result"]
* ```
*
* @see {@link acquireRelease} for resource acquisition with a release finalizer
* @see {@link ensuring} for attaching a finalizer to one effect
*
* @stability stable
* @category resource management
* @since 2.0.0
*/
const addFinalizer = addFinalizer$2;
/**
* Ensures that a cleanup function runs whether this effect succeeds, fails, or
* is interrupted.
*
* **Details**
*
* If both the effect and the cleanup function fail, the two causes are merged.
*
* **Example** (Observing every exit)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
* const output: Array<unknown> = []
*
* const task = Effect.succeed(42)
*
* const program = Effect.onExit(task, (exit) =>
*   Effect.sync(() => { output.push(
*     Exit.isSuccess(exit)
*       ? `Task succeeded with: ${exit.value}`
*       : `Task failed: ${Exit.isFailure(exit) ? exit.cause : "interrupted"}`
*   ) }))
*
* void output.push(Effect.runSync(program))
* output // => ["Task succeeded with: 42", 42]
* ```
*
* @stability stable
* @category resource management
* @since 2.0.0
*/
const onExit = onExit$1;
/**
* Returns an effect that is immediately interrupted.
*
* **Example** (Creating an interrupted effect)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const program = Effect.gen(function*() {
*   return yield* Effect.interrupt
*   yield* Effect.succeed("This won't execute and is unreachable")
* })
*
* Effect.runSyncExit(program)._tag // => "Failure"
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const interrupt = interrupt$1;
/**
* Runs the specified finalizer effect if this effect is interrupted.
*
* **Example** (Running cleanup on interruption)
*
* ```ts import.meta.vitest
* import { Effect, Fiber } from "effect"
* const output: Array<unknown> = []
*
* const task = Effect.forever(Effect.succeed("working..."))
*
* const program = Effect.onInterrupt(
*   task,
*   () => Effect.sync(() => { output.push("Task was interrupted, cleaning up...") })
* )
*
* const fiber = Effect.runFork(program)
* await Effect.runPromise(Fiber.interrupt(fiber))
* output // => ["Task was interrupted, cleaning up..."]
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const onInterrupt = onInterrupt$1;
/**
* Returns a new effect that disables interruption for the given effect.
*
* **Example** (Preventing interruption)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* const criticalTask = Effect.gen(function*() {
*   yield* Effect.sync(() => { output.push("Starting critical section...") })
*   yield* Effect.sync(() => { output.push("Critical section completed") })
* })
*
* const program = Effect.uninterruptible(criticalTask)
*
* Effect.runSync(program)
* output // => ["Starting critical section...", "Critical section completed"]
* ```
*
* @stability stable
* @category interruption
* @since 2.0.0
*/
const uninterruptible = uninterruptible$1;
/**
* Repeats this effect forever (until the first error).
*
* **Example** (Repeating forever)
*
* ```ts import.meta.vitest
* import { Effect, Option } from "effect"
*
* const program = Effect.forever(Effect.never).pipe(Effect.timeoutOption(0))
* await Effect.runPromise(program) // => Option.none()
* ```
*
* @stability stable
* @category repetition
* @since 2.0.0
*/
const forever = forever$2;
/**
* Repeats an effect based on a specified schedule or until the first failure.
*
* **When to use**
*
* Use to rerun an effect after successful executions.
*
* **Details**
*
* This function executes an effect repeatedly according to the given schedule.
* Each repetition occurs after the initial execution of the effect, meaning
* that the schedule determines the number of additional repetitions. For
* example, using `Schedule.once` will result in the effect being executed twice
* (once initially and once as part of the repetition).
*
* If the effect succeeds, it is repeated according to the schedule. If it
* fails, the repetition stops immediately, and the failure is returned.
*
* The schedule can also specify delays between repetitions, making it useful
* for tasks like retrying operations with backoff, periodic execution, or
* performing a series of dependent actions.
*
* You can combine schedules for more advanced repetition logic, such as adding
* delays, limiting recursions, or dynamically adjusting based on the outcome of
* each execution.
*
* **Gotchas**
*
* The source effect is always evaluated once before the schedule is stepped.
* The schedule controls additional repetitions, not the initial execution.
*
* If the effect or policy fails with a cause that contains a defect, the
* failure is propagated with every typed error converted to a defect.
* An interruption alongside a typed error does not block the typed error path.
*
* **Example** (Repeating successful effects with a schedule)
*
* ```ts import.meta.vitest
* // Success Example
* import { Effect, Schedule } from "effect"
* const output: Array<unknown> = []
*
* const action = Effect.sync(() => { output.push("success") })
* const policy = Schedule.recurs(2)
* const program = Effect.repeat(action, policy)
*
* void output.push(Effect.runSync(program))
* output // => ["success", "success", "success", 2]
* ```
*
* **Example** (Stopping repetition on failure)
*
* ```ts import.meta.vitest
* // Failure Example
* import { Effect, Schedule } from "effect"
* const output: Array<unknown> = []
*
* let count = 0
*
* // Define a callback effect that simulates an action with possible failures
* const action = Effect.callback<string, string>((resume) => {
*   if (count > 1) {
*     void output.push("failure")
*     resume(Effect.fail("Uh oh!"))
*   } else {
*     count++
*     void output.push("success")
*     resume(Effect.succeed("yay!"))
*   }
* })
*
* const policy = Schedule.recurs(2)
* const program = Effect.repeat(action, policy)
*
* void output.push((await Effect.runPromiseExit(program))._tag)
* output // => ["success", "success", "failure", "Failure"]
* ```
*
* @see {@link retry} for failure-based repetition
* @see {@link repeatOrElse} for fallback handling when repetition fails
*
* @stability stable
* @category repetition
* @since 2.0.0
*/
const repeat = repeat$1;
/**
* Forks the fiber in a `Scope`, interrupting it when the scope is closed.
*
* **Example** (Forking into the current scope)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const backgroundTask = Effect.never
*
* const program = Effect.scoped(
*   Effect.gen(function*() {
*     yield* backgroundTask.pipe(Effect.forkScoped)
*
*     // Fiber will be interrupted when scope closes
*     return "scope completed"
*   })
* )
*
* await Effect.runPromise(program) // => "scope completed"
* ```
*
* @stability stable
* @category forking
* @since 2.0.0
*/
const forkScoped = forkScoped$1;
/**
* Runs an effect in the background, returning a fiber that can
* be observed or interrupted.
*
* **When to use**
*
* Use when you need to start an effect in the background and receive a fiber.
*
* **Example** (Running an effect in the background)
*
* ```ts import.meta.vitest
* import { Effect, Fiber } from "effect"
* const output: Array<unknown> = []
*
* //      ┌─── Effect<string, never, never>
* //      ▼
* const program = Effect.sync(() => { output.push("running...") }).pipe(Effect.as("done"))
*
* //      ┌─── Fiber<string, never>
* //      ▼
* const fiber = Effect.runFork(program)
*
* void output.push(await Effect.runPromise(Fiber.join(fiber)))
* output // => ["running...", "done"]
* ```
*
* @stability stable
* @category running
* @since 2.0.0
*/
const runFork = runFork$1;
/**
* Executes an effect and returns the result as a `Promise`.
*
* **When to use**
*
* Use when you need to execute an effect and work with the
* result using `Promise` syntax, typically for compatibility with other
* promise-based code.
*
* If the effect succeeds, the promise will resolve with the result. If the
* effect fails, the promise will reject with an error.
*
* **Example** (Running a successful effect as a Promise)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* await Effect.runPromise(Effect.succeed(1)) // => 1
* ```
*
* **Example** (Running effects as promises)
*
* ```ts import.meta.vitest
* //Example: Handling a Failing Effect as a Rejected Promise
* import { Effect } from "effect"
* const output: Array<unknown> = []
*
* await Effect.runPromise(Effect.fail("my error")).catch(() => {
*   void output.push("rejected")
* })
* output // => ["rejected"]
* ```
*
* @see {@link runPromiseExit} for a version that returns an `Exit` type instead of rejecting.
* @stability stable
* @category running
* @since 2.0.0
*/
const runPromise = runPromise$1;
/**
* Runs an effect synchronously and captures the outcome safely as an `Exit` type, which
* represents the outcome (success or failure) of the effect.
*
* **When to use**
*
* Use to find out whether an effect succeeded or failed,
* including any defects, without dealing with asynchronous operations.
*
* **Details**
*
* The `Exit` type represents the result of the effect. Successful effects are
* wrapped in `Success`, and failed effects are wrapped in `Failure` with a
* `Cause`.
*
* If the effect contains asynchronous operations, `runSyncExit` will
* return an `Failure` with a `Die` cause, indicating that the effect cannot be
* resolved synchronously.
*
* **Example** (Observing synchronous results as Exit)
*
* ```ts import.meta.vitest
* import { Effect, Exit } from "effect"
*
* Effect.runSyncExit(Effect.succeed(1)) // => Exit.succeed(1)
*
* Effect.runSyncExit(Effect.fail("my error")) // => Exit.fail("my error")
* ```
*
* **Example** (Capturing async work as a Die cause)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Exit } from "effect"
*
* const exit = Effect.runSyncExit(Effect.promise(() => Promise.resolve(1)))
* const isAsyncDie = Exit.isFailure(exit) && Exit.hasDies(exit) && exit.cause.reasons.some(
*   (reason) => Cause.isDieReason(reason) && Cause.isAsyncFiberError(reason.defect)
* )
*
* isAsyncDie // => true
* ```
*
* @see {@link runSync} for a version that throws on failure.
*
* @stability stable
* @category running
* @since 2.0.0
*/
const runSyncExit = runSyncExit$1;
/**
* Applies `map` eagerly when an effect is already resolved.
*
* **When to use**
*
* Use when an already-resolved effect should apply a success transformation
* immediately while pending effects still use regular mapping.
*
* **Details**
*
* Success effects apply the mapping function immediately. Failure effects pass
* through unchanged, and pending effects fall back to regular `map` behavior.
*
* **Example** (Mapping already completed effects)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // For resolved effects, the mapping is applied immediately
* const resolved = Effect.succeed(5)
* const mapped = Effect.mapEager(resolved, (n) => n * 2) // Applied eagerly
*
* // For pending effects, behaves like regular map
* const pending = Effect.delay(Effect.succeed(5), 0)
* const mappedPending = Effect.mapEager(pending, (n) => n * 2) // Uses regular map
*
* await Effect.runPromise(Effect.all([mapped, mappedPending])) // => [10, 10]
* ```
*
* @stability stable
* @category mapping
* @since 4.0.0
*/
const mapEager = mapEager$1;
/**
* Applies `flatMap` eagerly when an effect is already resolved.
*
* **When to use**
*
* Use when an already-resolved successful effect should bind immediately to the
* next effect while pending effects still use regular flat mapping.
*
* **Details**
*
* Success effects apply the flatMap function immediately. Failure effects pass
* through unchanged, and pending effects fall back to regular `flatMap`
* behavior.
*
* **Example** (Flat mapping eagerly when possible)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* // For resolved effects, the flatMap is applied immediately
* const resolved = Effect.succeed(5)
* const flatMapped = Effect.flatMapEager(resolved, (n) => Effect.succeed(n * 2)) // Applied eagerly
*
* // For pending effects, behaves like regular flatMap
* const pending = Effect.delay(Effect.succeed(5), 0)
* const flatMappedPending = Effect.flatMapEager(
*   pending,
*   (n) => Effect.succeed(n * 2)
* ) // Uses regular flatMap
*
* await Effect.runPromise(Effect.all([flatMapped, flatMappedPending])) // => [10, 10]
* ```
*
* @stability stable
* @category sequencing
* @since 4.0.0
*/
const flatMapEager = flatMapEager$1;
/**
* Creates untraced function effects with eager evaluation optimization.
*
* **Details**
*
* Executes generator functions eagerly when all yielded effects are synchronous,
* stopping at the first async effect and deferring to normal execution.
* Additional arguments after the generator body act like `pipe` transforms, and
* `{ self }` before the body binds `this`, as with {@link fnUntraced}.
*
* **Example** (Defining eager untraced effect functions)
*
* ```ts import.meta.vitest
* import { Effect } from "effect"
*
* const computation = Effect.fnUntracedEager(function*() {
*   yield* Effect.succeed(1)
*   yield* Effect.succeed(2)
*   return "computed eagerly"
* })
*
* const effect = computation() // Executed immediately if all effects are sync
* Effect.runSync(effect) // => "computed eagerly"
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fnUntracedEager = fnUntracedEager$1;
// node_modules/effect/dist/BigDecimal.js
/**
* Decimal numbers and arithmetic for cases where JavaScript `number` rounding
* is not precise enough. A `BigDecimal` stores digits as a `bigint` plus a
* decimal scale, which lets the module parse, compare, add, subtract, multiply,
* divide, round, and format decimal values such as money, quantities, and
* measurements.
*
* Parsed scales are limited to ±10,000, and operations that align scales throw
* a `RangeError` when the scales differ by more than 20,000.
*
* @stability stable
* @since 2.0.0
*/
const TypeId$16 = "~effect/BigDecimal";
const BigDecimalProto = {
	[TypeId$16]: TypeId$16,
	[symbol$1]() {
		const normalized = normalize(this);
		return combine(string$1(String(normalized.value)), number$1(normalized.scale));
	},
	[symbol](that) {
		return isBigDecimal(that) && compare(this, that) === 0;
	},
	toString() {
		return `BigDecimal(${format(this)})`;
	},
	toJSON() {
		return {
			_id: "BigDecimal",
			value: String(this.value),
			scale: this.scale
		};
	},
	[NodeInspectSymbol]() {
		return this.toJSON();
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/**
* Checks whether a given value is a `BigDecimal`.
*
* **When to use**
*
* Use to validate unknown input and narrow it to `BigDecimal`.
*
* **Example** (Checking BigDecimal values)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* const decimal = BigDecimal.fromNumber(123.45)
* BigDecimal.isBigDecimal(decimal) // => false
* BigDecimal.isBigDecimal(BigDecimal.fromStringUnsafe("123.45")) // => true
* BigDecimal.isBigDecimal(123.45) // => false
* BigDecimal.isBigDecimal("123.45") // => false
* ```
*
* @stability stable
* @category guards
* @since 2.0.0
*/
const isBigDecimal = (u) => hasProperty(u, TypeId$16);
/**
* Creates a `BigDecimal` from a `bigint` value and a safe integer scale.
*
* **When to use**
*
* Use to construct a decimal directly from its unscaled integer value and
* decimal scale.
*
* **Gotchas**
*
* Throws a `RangeError` if `scale` is not a safe integer.
*
* **Example** (Creating decimals from bigint and scale)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* // Create 123.45 (12345 with scale 2)
* const decimal = BigDecimal.make(12345n, 2)
* decimal // => BigDecimal.fromStringUnsafe("123.45")
*
* // Create 42 (42 with scale 0)
* const integer = BigDecimal.make(42n, 0)
* integer // => BigDecimal.fromBigInt(42n)
* ```
*
* @see {@link fromBigInt} for constructing an integer decimal from a `bigint`
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$10 = (value, scale) => {
	if (!Number.isSafeInteger(scale)) throw new RangeError(`Scale must be a safe integer, got ${scale}`);
	const o = Object.create(BigDecimalProto);
	o.value = value;
	o.scale = scale;
	return o;
};
const makeNormalized = (value, scale) => {
	const o = make$10(value, scale);
	o.normalized = o;
	return o;
};
const bigint0 = /*#__PURE__*/ BigInt(0);
const bigint1 = /*#__PURE__*/ BigInt(1);
const bigint10 = /*#__PURE__*/ BigInt(10);
const zero = /*#__PURE__*/ makeNormalized(bigint0, 0);
/**
* Normalizes a given `BigDecimal` by removing trailing zeros.
*
* **When to use**
*
* Use to canonicalize decimals that have equivalent values but different
* internal scales.
*
* **Example** (Normalizing trailing zeros)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* const decimal = BigDecimal.normalize(BigDecimal.fromStringUnsafe("123.00000"))
* const decimalStorage = [decimal.value, decimal.scale] // => [123n, 0]
*
* const largeDecimal = BigDecimal.normalize(BigDecimal.fromStringUnsafe("12300000"))
* const largeDecimalStorage = [largeDecimal.value, largeDecimal.scale] // => [123n, -5]
* ```
*
* @see {@link format} for rendering normalized decimals as strings
*
* @stability stable
* @category scaling
* @since 2.0.0
*/
const normalize = (self) => {
	if (self.normalized === void 0) {
		if (self.value === bigint0) self.normalized = zero;
		else {
			const digits = `${self.value}`;
			const zeros = trailingZeros(digits);
			self.normalized = makeNormalized(BigInt(digits.slice(0, digits.length - zeros)), self.scale - zeros);
		}
	}
	return self.normalized;
};
const trailingZeros = (digits) => {
	let end = digits.length;
	while (digits[end - 1] === "0") end--;
	return digits.length - end;
};
const MAX_COMPARISON_SCALE_ALIGNMENT = 100;
const comparisonPowersOfTen = [bigint1];
const compareBigInt = (self, that) => self === that ? 0 : self < that ? -1 : 1;
const compareMagnitude = (self, that) => {
	const selfDigits = `${self.value < bigint0 ? -self.value : self.value}`;
	const thatDigits = `${that.value < bigint0 ? -that.value : that.value}`;
	const exponentDifference = BigInt(selfDigits.length - thatDigits.length) - BigInt(self.scale) + BigInt(that.scale);
	if (exponentDifference !== bigint0) return exponentDifference < bigint0 ? -1 : 1;
	const length = Math.max(selfDigits.length, thatDigits.length);
	return String$4(selfDigits.padEnd(length, "0"), thatDigits.padEnd(length, "0"));
};
const compare = (self, that) => {
	if (self.scale === that.scale) return compareBigInt(self.value, that.value);
	const selfSign = sign(self);
	const thatSign = sign(that);
	if (selfSign !== thatSign) return selfSign < thatSign ? -1 : 1;
	if (selfSign === 0) return 0;
	const scaleDifference = self.scale - that.scale;
	const absoluteScaleDifference = Math.abs(scaleDifference);
	if (absoluteScaleDifference > MAX_COMPARISON_SCALE_ALIGNMENT) return selfSign === -1 ? compareMagnitude(that, self) : compareMagnitude(self, that);
	const powerOfTen = comparisonPowersOfTen[absoluteScaleDifference] ??= bigint10 ** BigInt(absoluteScaleDifference);
	return scaleDifference > 0 ? compareBigInt(self.value, that.value * powerOfTen) : compareBigInt(self.value * powerOfTen, that.value);
};
/**
* Determines the sign of a given `BigDecimal`.
*
* **When to use**
*
* Use to classify a `BigDecimal` as negative, zero, or positive.
*
* **Example** (Reading decimal signs)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.sign(BigDecimal.fromStringUnsafe("-5")) // => -1
* BigDecimal.sign(BigDecimal.fromStringUnsafe("0")) // => 0
* BigDecimal.sign(BigDecimal.fromStringUnsafe("5")) // => 1
* ```
*
* @stability stable
* @category math
* @since 2.0.0
*/
const sign = (n) => n.value === bigint0 ? 0 : n.value < bigint0 ? -1 : 1;
/**
* Formats a `BigDecimal` as a string.
*
* **When to use**
*
* Use to render a `BigDecimal` as plain decimal text when possible.
*
* **Details**
*
* The value is normalized before formatting. Scientific notation is used when
* the absolute value of the normalized scale is at least `16`; otherwise plain
* decimal notation is used.
*
* **Example** (Formatting decimals)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.format(BigDecimal.fromStringUnsafe("-5")) // => "-5"
* BigDecimal.format(BigDecimal.fromStringUnsafe("123.456")) // => "123.456"
* BigDecimal.format(BigDecimal.fromStringUnsafe("-0.00000123")) // => "-0.00000123"
* ```
*
* @see {@link toExponential} for always rendering scientific notation
*
* @stability stable
* @category converting
* @since 2.0.0
*/
const format = (n) => {
	const normalized = normalize(n);
	if (Math.abs(normalized.scale) >= 16) return toExponential(normalized);
	const negative = normalized.value < bigint0;
	const absolute = `${negative ? -normalized.value : normalized.value}`;
	const digits = normalized.scale > 0 ? absolute.padStart(normalized.scale + 1, "0") : absolute.padEnd(absolute.length - normalized.scale, "0");
	const point = digits.length - normalized.scale;
	const complete = normalized.scale > 0 ? `${digits.slice(0, point)}.${digits.slice(point)}` : digits;
	return negative ? `-${complete}` : complete;
};
/**
* Formats a given `BigDecimal` as a `string` in scientific notation.
*
* **When to use**
*
* Use to render a `BigDecimal` in scientific notation.
*
* **Example** (Formatting decimals exponentially)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.toExponential(BigDecimal.make(123456n, -5)) // => "1.23456e+10"
* ```
*
* @see {@link format} for plain decimal formatting when possible
*
* @stability stable
* @category converting
* @since 3.11.0
*/
const toExponential = (n) => {
	if (isZero(n)) return "0e+0";
	const normalized = normalize(n);
	const digits = `${normalized.value}`;
	const point = normalized.value < bigint0 ? 2 : 1;
	const head = digits.slice(0, point);
	const tail = digits.slice(point);
	const exp = tail.length - normalized.scale;
	return `${head}${tail === "" ? "" : `.${tail}`}e${exp >= 0 ? "+" : ""}${exp}`;
};
/**
* Checks whether a given `BigDecimal` is `0`.
*
* **When to use**
*
* Use to test whether a `BigDecimal` is exactly zero.
*
* **Example** (Checking zero decimals)
*
* ```ts import.meta.vitest
* import { BigDecimal } from "effect"
*
* BigDecimal.isZero(BigDecimal.fromStringUnsafe("0")) // => true
* BigDecimal.isZero(BigDecimal.fromStringUnsafe("1")) // => false
* ```
*
* @stability stable
* @category predicates
* @since 2.0.0
*/
const isZero = (n) => n.value === bigint0;
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const codes = /*#__PURE__*/ (/* @__PURE__ */ new Uint8Array(123)).fill(255);
for (let i = 0; i < 64; i++) codes[/*#__PURE__*/ alphabet.charCodeAt(i)] = i;
codes[/*#__PURE__*/ "=".charCodeAt(0)] = 0;
({ ...BaseProto });
({ ...BaseProto });
({ ...PipeInspectableProto });
BaseProto.toString, BaseProto[NodeInspectSymbol];
({ ...PipeInspectableProto });
/** @internal */
const SENTINELS_ANNOTATION_KEY = "~sentinels";
/** @internal */
const CONSTRUCTOR_ANNOTATION_KEY = "~constructor";
// node_modules/effect/dist/internal/schema/parser.js
/** @internal */
const missing = /*#__PURE__*/ Symbol();
/** @internal */
const succeed = succeed$2;
/** @internal */
const missingExit = /*#__PURE__*/ succeed(missing);
/** @internal */
const sameExit = /*#__PURE__*/ succeed(missing);
/** @internal */
const toOption = (value) => value === missing ? none() : some(value);
/** @internal */
const fromOptionExit = (option) => option._tag === "None" ? missingExit : succeed(option.value);
// node_modules/effect/dist/SchemaIssue.js
/**
* Describes problems found while decoding, encoding, or checking data with
* schemas.
*
* An `Issue` records what failed and, for nested data, where the failure
* happened. The Schema system uses these values for missing keys, unexpected
* keys, invalid types, invalid values, failed filters, failed transformations,
* and alternatives that did not match. This module also formats issues.
*
* @stability stable
* @since 4.0.0
*/
const TypeId$11 = "~effect/SchemaIssue/Issue";
/**
* Returns `true` if the given value is an {@link Issue}.
*
* **When to use**
*
* Use when you need to narrow an `unknown` value to `Issue` in error-handling
* code, such as distinguishing an `Issue` from other error types in a catch-all
* handler.
*
* **Details**
*
* - Checks for the internal `TypeId` brand on the value.
*
* **Example** (Type-guarding an unknown error)
*
* ```ts import.meta.vitest
* import { SchemaIssue } from "effect"
*
* const issue = new SchemaIssue.MissingKey(undefined)
* SchemaIssue.isIssue(issue) // => true
* SchemaIssue.isIssue("not an issue") // => false
* ```
*
* @see {@link Issue}
*
* @stability stable
* @category guards
* @since 4.0.0
*/
function isIssue(u) {
	return hasProperty(u, TypeId$11) && u[TypeId$11] === TypeId$11;
}
var IssueNodeImpl = class {
	[TypeId$11] = TypeId$11;
	constructor(input, options) {
		if (options?.reportInput === true && input !== missing) this.input = input;
	}
};
/**
* Constructs a schema issue for a failed refinement check.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Filter$1 = class extends IssueNodeImpl {
	_tag = "Filter";
	/**
	* The filter that failed.
	*/
	filter;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(filter, issue, input, options) {
		super(input, options);
		this.filter = filter;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue for a failed transformation.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Encoding = class extends IssueNodeImpl {
	_tag = "Encoding";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(ast, issue, input, options) {
		super(input, options);
		this.ast = ast;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue that points to a nested location.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Pointer = class extends IssueNodeImpl {
	_tag = "Pointer";
	/**
	* The path to the location in the input that caused the issue.
	*/
	path;
	/**
	* The issue that occurred.
	*/
	issue;
	constructor(path, issue) {
		super();
		this.path = path;
		this.issue = issue;
	}
};
/**
* Constructs a schema issue for a missing key or tuple index.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const MissingKey = class extends IssueNodeImpl {
	_tag = "MissingKey";
	/**
	* The metadata for the issue.
	*/
	annotations;
	constructor(annotations) {
		super();
		this.annotations = annotations;
	}
};
/**
* Constructs a schema issue for an unexpected key or tuple index.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const UnexpectedKey = class extends IssueNodeImpl {
	_tag = "UnexpectedKey";
	/**
	* The schema that caused the issue.
	*/
	ast;
	constructor(ast, input, options) {
		super(input, options);
		this.ast = ast;
	}
};
/**
* Constructs a schema issue that groups multiple child issues.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Composite = class extends IssueNodeImpl {
	_tag = "Composite";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issues that occurred.
	*/
	issues;
	constructor(ast, issues, input, options) {
		super(input, options);
		this.ast = ast;
		this.issues = issues;
	}
};
/**
* Constructs a schema issue for an input with an invalid runtime type.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const InvalidType = class extends IssueNodeImpl {
	_tag = "InvalidType";
	/**
	* The schema that caused the issue.
	*/
	ast;
	constructor(ast, input, options) {
		super(input, options);
		this.ast = ast;
	}
};
/**
* Constructs a schema issue for a value that violates a constraint.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const InvalidValue = class extends IssueNodeImpl {
	_tag = "InvalidValue";
	/**
	* The metadata for the issue.
	*/
	annotations;
	constructor(annotations, input, options) {
		super(input, options);
		this.annotations = annotations;
	}
};
/**
* Constructs a schema issue for a value that matches no union member.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const AnyOf = class extends IssueNodeImpl {
	_tag = "AnyOf";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The issues that occurred.
	*/
	issues;
	constructor(ast, issues, input, options) {
		super(input, options);
		this.ast = ast;
		this.issues = issues;
	}
};
/**
* Constructs a schema issue for a value that matches multiple union members.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const OneOf = class extends IssueNodeImpl {
	_tag = "OneOf";
	/**
	* The schema that caused the issue.
	*/
	ast;
	/**
	* The schemas that were successful.
	*/
	successes;
	constructor(ast, successes, input, options) {
		super(input, options);
		this.ast = ast;
		this.successes = successes;
	}
};
function makeFilterIssue(entry, input, options) {
	if (isIssue(entry)) return entry;
	if (typeof entry === "string") return new InvalidValue({ message: entry }, input, options);
	const inner = typeof entry.issue === "string" ? new InvalidValue({ message: entry.issue }, input, options) : entry.issue;
	return new Pointer(entry.path, inner);
}
/** @internal */
function makeSingle(out, input, options) {
	if (out === void 0) return;
	if (typeof out === "boolean") return out ? void 0 : new InvalidValue(void 0, input, options);
	return makeFilterIssue(out, input, options);
}
/** @internal */
function normalizeFilterOutput(ast, out, input, options) {
	if (Array.isArray(out)) {
		if (!isReadonlyArrayNonEmpty(out)) return;
		return out.length === 1 ? makeFilterIssue(out[0], input, options) : new Composite(ast, map$3(out, (entry) => makeFilterIssue(entry, input, options)), input, options);
	}
	return makeSingle(out, input, options);
}
// node_modules/effect/dist/internal/schema/cause.js
/** @internal */
function getSchemaIssue(cause) {
	let issue;
	for (const reason of cause.reasons) {
		if (!isFailReason(reason) || !isIssue(reason.error)) return;
		issue ??= reason.error;
	}
	return issue;
}
/** @internal */
function getSchemaIssueOrThrow(cause, message) {
	const issue = getSchemaIssue(cause);
	if (issue === void 0) throw new Error(message, { cause });
	return issue;
}
// node_modules/effect/dist/SchemaGetter.js
const makeGetter = (fields) => Object.assign(Object.create(Prototype$1), fields);
const passthrough_$1 = /*#__PURE__*/ makeGetter({ _tag: "Passthrough" });
function passthrough$1() {
	return passthrough_$1;
}
/**
* Creates a getter that applies a pure function to present values.
*
* **When to use**
*
* Use when you need a schema getter for a pure, infallible transformation
* between types.
* - Building encode/decode pairs for `Schema.decodeTo`.
*
* **Details**
*
* - This is the most commonly used constructor.
* - Transforms `Some(e)` to `Some(f(e))` and leaves `None` unchanged.
* - Skips `None` inputs — only called when a value is present.
* - Never fails.
*
* **Example** (Transforming strings to numbers)
*
* ```ts import.meta.vitest
* import { Schema, SchemaGetter } from "effect"
*
* const NumberFromString = Schema.String.pipe(
*   Schema.decodeTo(Schema.Number, {
*     decode: SchemaGetter.transform((s) => Number(s)),
*     encode: SchemaGetter.transform((n) => String(n))
*   })
* )
* Schema.decodeSync(NumberFromString)("42") // => 42
* ```
*
* @see {@link transformEffect} when the transformation returns an `Effect`
* @see {@link transformOptional} when you need to handle `None` inputs
* @see {@link passthrough} when no transformation is needed
*
* @stability stable
* @category transforming
* @since 4.0.0
*/
function transform$1(f) {
	return makeGetter({
		_tag: "Transform",
		transform: f
	});
}
/**
* Coerces any value to a `string` using the global `String()` constructor.
*
* **When to use**
*
* Use when you need a schema getter to coerce a present encoded value to a
* string with `String()`.
*
* **Details**
*
* The getter is pure, never fails, and delegates to `globalThis.String`.
*
* **Example** (Coercing to a string)
*
* ```ts import.meta.vitest
* import { Effect, Option, SchemaGetter } from "effect"
*
* const toString = SchemaGetter.String<number>()
* Effect.runSync(SchemaGetter.run(toString, Option.some(42), {})) // => Option.some("42")
* ```
*
* @see {@link transform} for custom string conversions
*
* @stability stable
* @category converting
* @since 4.0.0
*/
function String$3() {
	return transform$1(globalThis.String);
}
/**
* Coerces any value to a `number` using the global `Number()` constructor.
*
* **When to use**
*
* Use when you need a schema getter to coerce a present encoded value to a
* number with `Number()`.
*
* **Details**
*
* The getter is pure, never fails, and delegates to `globalThis.Number`. It may
* produce `NaN` for non-numeric inputs.
*
* **Example** (Coercing to a number)
*
* ```ts import.meta.vitest
* import { Effect, Option, SchemaGetter } from "effect"
*
* const toNumber = SchemaGetter.Number<string>()
* Effect.runSync(SchemaGetter.run(toNumber, Option.some("42"), {})) // => Option.some(42)
* ```
*
* @see {@link transformEffect} for effectful or validated number parsing
*
* @stability stable
* @category converting
* @since 4.0.0
*/
function Number$3() {
	return transform$1(globalThis.Number);
}
// node_modules/effect/dist/SchemaTransformation.js
/**
* Constructs schema middleware from its decode and encode functions.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Middleware = class extends Class$1 {
	_tag = "Middleware";
	decode;
	encode;
	constructor(decode, encode) {
		super();
		this.decode = decode;
		this.encode = encode;
	}
	flip() {
		return new Middleware(this.encode, this.decode);
	}
};
const TypeId$10 = "~effect/SchemaTransformation/Transformation";
/**
* Constructs a bidirectional schema transformation from its decode and encode getters.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Transformation = class extends Class$1 {
	[TypeId$10] = TypeId$10;
	_tag = "Transformation";
	decode;
	encode;
	constructor(decode, encode) {
		super();
		this.decode = decode;
		this.encode = encode;
	}
	flip() {
		return new Transformation(this.encode, this.decode);
	}
};
/**
* Returns `true` if `u` is a `Transformation` instance.
*
* **When to use**
*
* Use to check whether a value is already a schema transformation before
* wrapping it.
*
* **Details**
*
* - Pure predicate, no side effects.
* - Acts as a TypeScript type guard.
*
* **Example** (Checking a value)
*
* ```ts import.meta.vitest
* import { SchemaTransformation } from "effect"
*
* SchemaTransformation.isTransformation(SchemaTransformation.trim()) // => true
* SchemaTransformation.isTransformation({ decode: null, encode: null }) // => false
* ```
*
* @see {@link Transformation}
* @see {@link makeTransformation}
*
* @stability stable
* @category guards
* @since 4.0.0
*/
function isTransformation(u) {
	return hasProperty(u, TypeId$10) && u[TypeId$10] === TypeId$10;
}
/**
* Constructs a `Transformation` from an object with `decode` and `encode`
* `Getter`s. If the input is already a `Transformation`, returns it as-is.
*
* **When to use**
*
* Use when you already have schema getter instances and want to pair them into
* a schema transformation.
* - You want idempotent wrapping (won't double-wrap).
*
* **Details**
*
* - Returns the input unchanged if it is already a `Transformation`.
*
* **Example** (Wrapping existing getters)
*
* ```ts import.meta.vitest
* import { SchemaGetter, SchemaTransformation } from "effect"
*
* const t = SchemaTransformation.makeTransformation({
*   decode: SchemaGetter.transform<number, string>((s) => Number(s)),
*   encode: SchemaGetter.transform<string, number>((n) => String(n))
* })
* t._tag // => "Transformation"
* ```
*
* @see {@link transform} — simpler constructor from pure functions
* @see {@link transformEffect} — constructor from effectful functions
* @see {@link Transformation}
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const makeTransformation = (options) => {
	if (isTransformation(options)) return options;
	return new Transformation(options.decode, options.encode);
};
/**
* Creates a `Transformation` from pure (sync, infallible) decode and encode
* functions.
*
* **When to use**
*
* Use when you need an infallible schema transformation that does not require
* Effect services.
*
* **Details**
*
* - Each function receives the input and returns the output directly.
* - Skips `None` inputs (missing keys) — functions are only called on present values.
* - Does not allocate Effects internally; uses optimized sync path.
*
* **Example** (Converting between cents and dollars)
*
* ```ts import.meta.vitest
* import { Schema, SchemaTransformation } from "effect"
*
* const CentsFromDollars = Schema.Number.pipe(
*   Schema.decodeTo(
*     Schema.Number,
*     SchemaTransformation.transform({
*       decode: (dollars) => dollars * 100,
*       encode: (cents) => cents / 100
*     })
*   )
* )
* Schema.decodeSync(CentsFromDollars)(2.5) // => 250
* ```
*
* @see {@link transformEffect} — for fallible or effectful transformations
* @see {@link transformOptional} — for transformations that handle missing keys
* @see {@link passthrough} — when no conversion is needed
*
* @stability stable
* @category transforming
* @since 3.10.0
*/
function transform(options) {
	return new Transformation(transform$1(options.decode), transform$1(options.encode));
}
const passthrough_ = /*#__PURE__*/ new Transformation(/*#__PURE__*/ passthrough$1(), /*#__PURE__*/ passthrough$1());
function passthrough() {
	return passthrough_;
}
/**
* Decodes a `string` into a `number` and encodes a `number` back to a
* `string`.
*
* **When to use**
*
* Use when you need a schema transformation to parse numeric strings from APIs,
* form data, or URL parameters.
*
* **Details**
*
* Decoding coerces the string to a number like `Number(s)`. Encoding coerces
* the number to a string like `String(n)`. This does not validate that the
* result is finite; combine with `Schema.Finite` or `Schema.Int` for stricter
* checks.
*
* **Example** (Converting a string to a number)
*
* ```ts import.meta.vitest
* import { Schema, SchemaTransformation } from "effect"
*
* const schema = Schema.String.pipe(
*   Schema.decodeTo(Schema.Number, SchemaTransformation.numberFromString)
* )
* Schema.decodeSync(schema)("42") // => 42
* ```
*
* @see {@link bigintFromString}
* @see {@link transform}
*
* @stability stable
* @category converting
* @since 4.0.0
*/
const numberFromString = /*#__PURE__*/ new Transformation(/*#__PURE__*/ Number$3(), /*#__PURE__*/ String$3());
const isJsonError = (input) => isObject(input) && typeof input["message"] === "string";
const decodeJsonError = (input) => {
	const err = Object.hasOwn(input, "cause") ? new Error(input.message, { cause: decodeDefect(input.cause) }) : new Error(input.message);
	if (typeof input.name === "string" && input.name !== "Error") err.name = input.name;
	if (typeof input.stack === "string") err.stack = input.stack;
	return err;
};
const encodeUnknownAsJson = (input) => {
	try {
		const json = formatJson(input);
		return json === void 0 ? format$1(input) : JSON.parse(json);
	} catch {
		return format$1(input);
	}
};
const encodeJsonError = (input, options, encodeDefect) => {
	const encoded = {
		name: input.name,
		message: typeof input.message === "string" ? input.message : ""
	};
	if (options?.includeStack && typeof input.stack === "string") encoded.stack = input.stack;
	if (!options?.excludeCause && input.cause !== void 0) encoded.cause = encodeDefect(input.cause);
	return encoded;
};
const makeEncodeDefect = (options) => {
	const seen = /* @__PURE__ */ new WeakSet();
	const encode = (input) => {
		if (isError(input)) {
			if (seen.has(input)) return "[Circular]";
			seen.add(input);
			const encoded = encodeJsonError(input, options, encode);
			seen.delete(input);
			return encoded;
		}
		return encodeUnknownAsJson(input);
	};
	return encode;
};
const decodeDefect = (input) => isJsonError(input) ? decodeJsonError(input) : input;
/** @internal */
const defectFromJson = (options) => transform({
	decode: decodeDefect,
	encode: makeEncodeDefect(options)
});
// node_modules/effect/dist/SchemaAST.js
/**
* Represents Effect schemas as runtime trees.
*
* Every `Schema` has an AST made from nodes for declarations, primitives,
* literals, arrays, objects, unions, suspended schemas, checks, annotations,
* encoding links, and parsing context. Most users work with the higher-level
* `Schema` module. Use `SchemaAST` when you need to inspect schema nodes, build
* ASTs programmatically, change encoded or decoded views, collect issues, or
* run low-level schema checks.
*
* @stability stable
* @since 4.0.0
*/
function makeGuard(tag) {
	return (ast) => ast._tag === tag;
}
/**
* Narrows an {@link AST} to {@link Declaration}.
*
* **When to use**
*
* Use to recognize declaration AST nodes before running declaration-specific
* handling.
*
* @see {@link Declaration} for the AST node type narrowed by this guard
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isDeclaration = /*#__PURE__*/ makeGuard("Declaration");
/**
* Narrows an {@link AST} to {@link Never}.
*
* **When to use**
*
* Use to detect the AST node for a schema that can never match before handling
* other schema variants.
*
* @see {@link Never} for the AST node type narrowed by this guard
* @see {@link never} for the singleton `Never` AST instance
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isNever = /*#__PURE__*/ makeGuard("Never");
/**
* Narrows an {@link AST} to {@link Literal}.
*
* **When to use**
*
* Use to recognize exact string, number, boolean, or bigint literal AST nodes.
*
* @see {@link Literal} for the AST node type narrowed by this guard
* @see {@link LiteralValue} for the values stored by literal nodes
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isLiteral = /*#__PURE__*/ makeGuard("Literal");
/**
* Narrows an {@link AST} to {@link UniqueSymbol}.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isUniqueSymbol = /*#__PURE__*/ makeGuard("UniqueSymbol");
/**
* Narrows an {@link AST} to {@link Arrays}.
*
* **When to use**
*
* Use to recognize array-like AST nodes before reading their element, rest, or
* mutability metadata.
*
* @see {@link Arrays} for the AST node type narrowed by this guard
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isArrays = /*#__PURE__*/ makeGuard("Arrays");
/**
* Narrows an {@link AST} to {@link Objects}.
*
* @stability stable
* @category guards
* @since 4.0.0
*/
const isObjects = /*#__PURE__*/ makeGuard("Objects");
/**
* Narrows an {@link AST} to {@link Suspend}.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
const isSuspend = /*#__PURE__*/ makeGuard("Suspend");
/**
* Constructs a {@link Link}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Link = class {
	to;
	transformation;
	constructor(to, transformation) {
		this.to = to;
		this.transformation = transformation;
	}
};
/** @internal */
const defaultParseOptions = {};
/**
* Constructs a {@link Context}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Context = class {
	isOptional;
	isMutable;
	/** Used for constructor default values (e.g. `withConstructorDefault` API) */
	constructorDefault;
	annotations;
	constructor(isOptional, isMutable, constructorDefault = void 0, annotations = void 0) {
		this.isOptional = isOptional;
		this.isMutable = isMutable;
		this.constructorDefault = constructorDefault;
		this.annotations = annotations;
	}
};
const TypeId$9 = "~effect/Schema";
var ASTNodeImpl = class {
	[TypeId$9] = TypeId$9;
	annotations;
	checks;
	encoding;
	context;
	constructor(annotations = void 0, checks = void 0, encoding = void 0, context = void 0) {
		this.annotations = annotations;
		this.checks = checks;
		this.encoding = encoding;
		this.context = context;
	}
	toString() {
		return `<${this._tag}>`;
	}
};
/**
* Constructs a {@link Declaration}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Declaration = class extends ASTNodeImpl {
	_tag = "Declaration";
	typeParameters;
	run;
	encodingChecks;
	/**
	* Parser factory {@link flip} swaps in, so a declaration can behave
	* differently when encoding. `undefined` reuses `run`.
	*/
	encodingRun;
	constructor(typeParameters, run, annotations, checks, encoding, context, encodingChecks, encodingRun) {
		super(annotations, checks, encoding, context);
		this.typeParameters = typeParameters;
		this.run = run;
		this.encodingChecks = encodingChecks;
		this.encodingRun = encodingRun;
	}
	/** @internal */
	getParser() {
		let run;
		return (input, options) => {
			if (input === missing) return missingExit;
			run ??= this.run(this.typeParameters);
			try {
				return run(input, this, options);
			} catch (error) {
				return die(error);
			}
		};
	}
	_rebuild(recur, checks, encodingChecks, run, encodingRun) {
		const tps = mapOrSame(this.typeParameters, recur);
		return tps === this.typeParameters && checks === this.checks && encodingChecks === this.encodingChecks && run === this.run && encodingRun === this.encodingRun ? this : new Declaration(tps, run, this.annotations, checks, void 0, this.context, encodingChecks, encodingRun);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks, this.run, this.encodingRun);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks, this.encodingRun ?? this.run, this.run);
	}
	/** @internal */
	getExpected() {
		const expected = this.annotations?.expected;
		if (typeof expected === "string") return expected;
		return "<Declaration>";
	}
};
/**
* Constructs a {@link Unknown}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Unknown$1 = class extends ASTNodeImpl {
	_tag = "Unknown";
	/** @internal */
	getParser() {
		return fromRefinement(this, isUnknown);
	}
	/** @internal */
	getExpected() {
		return "unknown";
	}
};
/**
* Provides the singleton {@link Unknown} AST instance.
*
* **When to use**
*
* Use when you need the reusable AST singleton for a schema node that accepts
* every value while keeping parsed values opaque.
*
* @see {@link any} for the singleton that accepts every value as `any`
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const unknown = /*#__PURE__*/ new Unknown$1();
/**
* Constructs a {@link Literal}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Literal$1 = class extends ASTNodeImpl {
	_tag = "Literal";
	literal;
	constructor(literal, annotations, checks, encoding, context) {
		super(annotations, checks, encoding, context);
		if (typeof literal === "number" && !globalThis.Number.isFinite(literal)) throw new Error(`A numeric literal must be finite, got ${format$1(literal)}`);
		this.literal = literal;
	}
	/** @internal */
	getParser() {
		return fromConst(this, this.literal);
	}
	/** @internal */
	matchPart(s, _options) {
		return s === globalThis.String(this.literal) ? this.literal : void 0;
	}
	/** @internal */
	toCodecJson() {
		return typeof this.literal === "bigint" ? literalToString(this) : this;
	}
	/** @internal */
	toCodecStringTree() {
		return typeof this.literal === "string" ? this : literalToString(this);
	}
	/** @internal */
	getExpected() {
		return typeof this.literal === "string" ? JSON.stringify(this.literal) : globalThis.String(this.literal);
	}
};
function literalToString(ast) {
	const literalAsString = globalThis.String(ast.literal);
	return replaceEncoding(ast, [new Link(new Literal$1(literalAsString), new Transformation(transform$1(() => ast.literal), transform$1(() => literalAsString)))]);
}
/**
* Constructs a {@link String}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const String$2 = class extends ASTNodeImpl {
	_tag = "String";
	/** @internal */
	getParser() {
		return fromRefinement(this, isString$1);
	}
	/** @internal */
	matchPart(s, options) {
		const checks = this.checks;
		return checks && !options.disableChecks && collectIssues(checks, s, void 0, this, options) ? void 0 : s;
	}
	/** @internal */
	getExpected() {
		return "string";
	}
};
/**
* Provides the singleton {@link String} AST instance.
*
* **When to use**
*
* Use as the shared `SchemaAST` node for unconstrained JavaScript strings.
*
* @see {@link String} for the AST node class
* @see {@link isString} for narrowing an AST to a string node
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const string = /*#__PURE__*/ new String$2();
/**
* Constructs a {@link Number}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Number$2 = class extends ASTNodeImpl {
	_tag = "Number";
	/** @internal */
	getParser() {
		return fromRefinement(this, isNumber);
	}
	/** @internal */
	matchKey(s, options) {
		return this._match(isStringNumberRegExp, s, options);
	}
	/** @internal */
	matchPart(s, options) {
		return this._match(isStringFiniteRegExp, s, options);
	}
	_match(regexp, s, options) {
		if (!regexp.test(s)) return void 0;
		const value = globalThis.Number(s);
		if (options.disableChecks || !this.checks) return value;
		return collectIssues(this.checks, value, void 0, this, options) ? void 0 : value;
	}
	/** @internal */
	toCodecJson() {
		if (this.checks && (hasCheck(this.checks, "effect/schema/isFinite") || hasCheck(this.checks, "effect/schema/isInt"))) return this;
		return replaceEncoding(this, [numberToJson]);
	}
	/** @internal */
	toCodecStringTree() {
		if (this.toCodecJson() === this) return replaceEncoding(this, [finiteToString]);
		return replaceEncoding(this, [numberToString]);
	}
	/** @internal */
	getExpected() {
		return "number";
	}
};
function hasCheck(checks, id) {
	return checks.some((check) => check.annotations?.representation?.id === id || check._tag === "FilterGroup" && hasCheck(check.checks, id));
}
/**
* Provides the singleton {@link Number} AST instance.
*
* **When to use**
*
* Use when you need the canonical `SchemaAST` node for schemas that accept any
* JavaScript number value.
*
* @see {@link Number} for the AST node class and serialization behavior
* @see {@link Literal} for exact finite numeric literal AST nodes
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const number = /*#__PURE__*/ new Number$2();
/**
* Constructs a {@link Arrays}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Arrays = class extends ASTNodeImpl {
	_tag = "Arrays";
	isMutable;
	elements;
	rest;
	encodingChecks;
	constructor(isMutable, elements, rest, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.isMutable = isMutable;
		this.elements = elements;
		this.rest = rest;
		this.encodingChecks = encodingChecks;
		let hasOptional = false;
		for (let i = 0; i < elements.length; i++) if (isOptional(elements[i])) hasOptional = true;
		else if (hasOptional) throw new Error("A required element cannot follow an optional element. ts(1257)");
		if (hasOptional && rest.length > 1) throw new Error("A required element cannot follow an optional element. ts(1257)");
		for (let i = 1; i < rest.length; i++) if (isOptional(rest[i])) throw new Error("An optional element cannot follow a rest element. ts(1266)");
	}
	/** @internal */
	getParser(compile, compileField = compile) {
		const ast = this;
		let elements;
		let rest;
		const elementLen = ast.elements.length;
		const tailLen = Math.max(0, ast.rest.length - 1);
		function getParser(tailThreshold, index) {
			if (index < elementLen) return elements[index];
			else if (index >= tailThreshold) return rest[index - tailThreshold + 1];
			return rest[0];
		}
		const finish = (state) => {
			const { input, len, options } = state;
			if (ast.rest.length === 0 && len > elementLen) for (let i = elementLen; i < len; i++) {
				const unexpected = new UnexpectedKey(ast, input[i], options);
				const issue = new Pointer([i], unexpected);
				if (options.errors === "all") {
					if (state.issues) state.issues.push(issue);
					else state.issues = [issue];
				} else return fail$1(new Composite(ast, [issue], input, options));
			}
			if (state.issues) return fail$1(new Composite(ast, state.issues, input, options));
			return succeed(state.output);
		};
		const parse = (input, options) => {
			const len = input.length;
			const state = {
				ast,
				getParser,
				input,
				len,
				tailThreshold: Math.max(elementLen, len - tailLen),
				output: [],
				issues: void 0,
				options
			};
			const end = ast.rest.length === 0 ? elementLen : Math.max(len, elementLen + tailLen);
			const concurrency = options.concurrency === void 0 ? 1 : resolveConcurrency(options.concurrency);
			const eff = concurrency === 1 ? parseArray(state, input, 0, end) : parseArrayConcurrent(state, input, {
				concurrency,
				end
			});
			if (!eff) return finish(state);
			if (effectIsExit(eff)) return flatMapEager(eff, () => finish(state));
			let first = true;
			return suspend(() => {
				if (!first) return parse(input, options);
				first = false;
				return flatMap(eff, () => finish(state));
			});
		};
		return (input, options) => {
			if (input === missing) return missingExit;
			try {
				if (!Array.isArray(input)) return fail$1(new InvalidType(ast, input, options));
				if (!elements) {
					elements = ast.elements.map((ast) => ({
						ast,
						parser: compileField(ast)
					}));
					rest = ast.rest.map((ast) => ({
						ast,
						parser: compileField(ast)
					}));
				}
				return parse(input, options);
			} catch (error) {
				return die(error);
			}
		};
	}
	_rebuild(recur, checks, encodingChecks) {
		const elements = mapOrSame(this.elements, recur);
		const rest = mapOrSame(this.rest, recur);
		return elements === this.elements && rest === this.rest && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Arrays(this.isMutable, elements, rest, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	getExpected() {
		return "array";
	}
};
/** @internal */
function stepArray(s, item, exit, i) {
	if (exit._tag === "Failure") return wrapPropertyKeyIssue(s, s.ast, i, exit);
	const value = exit === sameExit ? item : exit[args];
	if (value !== missing) s.output[i] = value;
	else {
		const p = s.getParser(s.tailThreshold, i);
		if (isOptional(p.ast)) return;
		const issue = new Pointer([i], new MissingKey(p.ast.context?.annotations));
		if (s.options.errors === "all") {
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
		} else return fail$2(new Composite(s.ast, [issue], s.input, s.options));
	}
}
const parseArrayOptions = {
	onItem(s, item, i) {
		const value = i < s.len ? item : missing;
		return s.getParser(s.tailThreshold, i).parser(value, s.options);
	},
	step: stepArray
};
/** @internal */
const parseArray = /*#__PURE__*/ iterateEager()(parseArrayOptions);
const parseArrayConcurrent = /*#__PURE__*/ iterateConcurrent()(parseArrayOptions);
const wrapPropertyKeyIssue = (s, ast, key, exit) => {
	if (exit.cause.reasons.length === 0) return exit;
	const issue = getSchemaIssue(exit.cause);
	if (issue === void 0) return failCause$2(map$1(exit.cause, (issue) => new Composite(ast, [new Pointer([key], issue)], s.input, s.options)));
	const pointer = new Pointer([key], issue);
	if (s.options.errors === "all") {
		if (s.issues) s.issues.push(pointer);
		else s.issues = [pointer];
	} else return fail$2(new Composite(ast, [pointer], s.input, s.options));
};
/**
* floating point or integer, with optional exponent
*
* The mantissa alternation keeps rejection linear: `\d*\.?\d+` retries
* every split of a long digit run.
* @internal
*/
const FINITE_PATTERN = "[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[Ee][+-]?\\d+)?";
/**
* Returns the object keys that match the index signature parameter schema.
* @internal
*/
function getIndexSignatureKeys(input, parameter, options = defaultParseOptions) {
	let stringKeys;
	let symbolKeys;
	function go(parameter) {
		switch (parameter._tag) {
			case "String":
			case "TemplateLiteral": return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchPart(k, options) !== void 0);
			case "Number": return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchKey(k, options) !== void 0);
			case "Symbol": return (symbolKeys ??= Object.getOwnPropertySymbols(input)).filter((k) => Object.prototype.propertyIsEnumerable.call(input, k) && parameter.matchKey(k, options) !== void 0);
			case "Union": return [...new Set(parameter.types.flatMap(go))];
			default: return [];
		}
	}
	return go(parameterFromPropertyKey(toEncoded(parameter)));
}
/**
* Constructs a {@link PropertySignature}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const PropertySignature = class {
	name;
	type;
	constructor(name, type) {
		this.name = name;
		this.type = type;
	}
};
function isIndexSignatureParameterSide(ast) {
	switch (ast._tag) {
		case "String":
		case "Number":
		case "Symbol":
		case "TemplateLiteral": return true;
		case "Union": return ast.types.every(isIndexSignatureParameterSide);
		default: return false;
	}
}
function isIndexSignatureParameterEncodedSide(ast) {
	const encoded = getLastEncoding(ast);
	switch (encoded._tag) {
		case "String":
		case "Number":
		case "Symbol":
		case "TemplateLiteral": return true;
		case "Union": return encoded.types.every(isIndexSignatureParameterEncodedSide);
		default: return false;
	}
}
function isIndexSignatureParameter(ast) {
	return isIndexSignatureParameterSide(ast) && isIndexSignatureParameterEncodedSide(ast);
}
/**
* Constructs a {@link IndexSignature}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const IndexSignature = class {
	parameter;
	type;
	constructor(parameter, type) {
		if (!isIndexSignatureParameter(parameter)) throw new Error(`Invalid index signature parameter ${parameter._tag}`);
		this.parameter = parameter;
		this.type = type;
		if (isOptional(type) || type.encoding !== void 0 && isOptional(toEncoded(type))) throw new Error("Cannot use `Schema.optionalKey` or `Schema.optional` with index signatures. Index signatures already allow keys to be absent; use `Schema.UndefinedOr` to allow `undefined` values.");
	}
};
/**
* Constructs a {@link Objects}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Objects = class extends ASTNodeImpl {
	_tag = "Objects";
	propertySignatures;
	indexSignatures;
	encodingChecks;
	constructor(propertySignatures, indexSignatures, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.propertySignatures = propertySignatures;
		this.indexSignatures = indexSignatures;
		this.encodingChecks = encodingChecks;
	}
	/** @internal */
	getParser(compile, compileField = compile) {
		const ast = this;
		const hasProperties = ast.propertySignatures.length;
		const indexCount = ast.indexSignatures.length;
		if (!hasProperties && !indexCount) return fromRefinement(ast, isNotNullish);
		let properties;
		let indexes;
		const compileMembers = () => {
			if (!properties) {
				properties = ast.propertySignatures.map((ps) => ({
					parser: compileField(ps.type),
					name: ps.name,
					type: ps.type
				}));
				indexes = indexCount ? ast.indexSignatures.map((is) => ({
					is,
					parserKey: compile(parameterFromPropertyKey(is.parameter)),
					parserValue: compileField(is.type)
				})) : void 0;
			}
			return properties;
		};
		const makeFallback = () => {
			const expectedKeys = new Set(ast.propertySignatures.map((ps) => typeof ps.name === "number" ? globalThis.String(ps.name) : ps.name));
			const finishIndex = (s, key, k2, inputValue, exitValue) => {
				if (exitValue._tag === "Failure") return wrapPropertyKeyIssue(s, ast, key, exitValue) ?? void_$1;
				const value = exitValue === sameExit ? inputValue : exitValue[args];
				if (k2 !== missing && value !== missing) {
					if (hasProperties && (expectedKeys.has(key) || expectedKeys.has(typeof k2 === "number" ? globalThis.String(k2) : k2))) return void_$1;
					assignProperty(s.out, k2, value);
				}
				return void_$1;
			};
			const parseIndex = (s, key, index, exitKey) => {
				if (!exitKey) {
					const eff = index.parserKey(key, s.options);
					if (!effectIsExit(eff)) return flatMap(exit(eff), (exit) => parseIndex(s, key, index, exit));
					exitKey = eff;
				}
				if (exitKey._tag === "Failure") return wrapPropertyKeyIssue(s, ast, key, exitKey) ?? void_$1;
				const k2 = exitKey === sameExit ? key : exitKey[args];
				const inputValue = s.input[key];
				const result = index.parserValue(inputValue, s.options);
				return effectIsExit(result) ? finishIndex(s, key, k2, inputValue, result) : flatMap(exit(result), (exit) => finishIndex(s, key, k2, inputValue, exit));
			};
			const parseStringIndex = (s, key, index) => {
				const inputValue = s.input[key];
				const result = index.parserValue(inputValue, s.options);
				return effectIsExit(result) ? finishIndex(s, key, key, inputValue, result) : flatMap(exit(result), (exit) => finishIndex(s, key, key, inputValue, exit));
			};
			const parseIndexes = indexCount ? iterateConcurrent()({
				onItem: (s, [key, index]) => index.is.parameter === string ? parseStringIndex(s, key, index) : parseIndex(s, key, index),
				step: (_s, _item, exit) => exit._tag === "Failure" ? exit : void 0
			}) : void 0;
			return fnUntracedEager(function* (input, options) {
				if (input === missing) return missing;
				if (!(typeof input === "object" && input !== null && !Array.isArray(input))) return yield* fail$1(new InvalidType(ast, input, options));
				compileMembers();
				const record = input;
				const out = {};
				const state = {
					ast,
					input: record,
					out,
					issues: void 0,
					options
				};
				const errorsAllOption = options.errors === "all";
				const onExcessPropertyError = options.onExcessProperty === "error";
				const concurrency = options.concurrency === void 0 ? 1 : resolveConcurrency(options.concurrency);
				const indexKeys = indexCount && onExcessPropertyError ? ast.indexSignatures.map((index) => getIndexSignatureKeys(record, index.parameter, options)) : void 0;
				if (onExcessPropertyError) {
					const coveredKeys = indexKeys ? new Set(expectedKeys) : expectedKeys;
					if (indexKeys) for (const keys of indexKeys) for (const key of keys) coveredKeys.add(key);
					const inputKeys = Reflect.ownKeys(record);
					for (let i = 0; i < inputKeys.length; i++) {
						const key = inputKeys[i];
						if (!coveredKeys.has(key) && Object.prototype.propertyIsEnumerable.call(record, key)) {
							const unexpected = new UnexpectedKey(ast, record[key], options);
							const issue = new Pointer([key], unexpected);
							if (errorsAllOption) {
								if (state.issues) state.issues.push(issue);
								else state.issues = [issue];
								continue;
							} else return yield* fail$1(new Composite(ast, [issue], input, options));
						}
					}
				}
				if (hasProperties) {
					const eff = concurrency === 1 ? parseProperties(state, properties) : parsePropertiesConcurrent(state, properties, { concurrency });
					if (eff) yield* eff;
				}
				if (indexCount && concurrency === 1) for (let i = 0; i < indexCount; i++) {
					const index = indexes[i];
					const parse = index.is.parameter === string ? parseStringIndex : parseIndex;
					const keys = indexKeys?.[i] ?? (index.is.parameter === string ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
					for (let j = 0; j < keys.length; j++) {
						const eff = parse(state, keys[j], index);
						if (!effectIsExit(eff)) yield* eff;
						else if (eff._tag === "Failure") return yield* eff;
					}
				}
				else if (parseIndexes) {
					const keyPairs = empty();
					for (let i = 0; i < indexCount; i++) {
						const index = indexes[i];
						const keys = indexKeys?.[i] ?? (index.is.parameter === string ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
						for (let j = 0; j < keys.length; j++) keyPairs.push([keys[j], index]);
					}
					const eff = parseIndexes(state, keyPairs, { concurrency });
					if (eff) yield* eff;
				}
				if (state.issues) return yield* fail$1(new Composite(ast, state.issues, input, options));
				return out;
			});
		};
		if (indexCount) return makeFallback();
		let fallback;
		const resume = (snapshot, index, pending) => {
			const property = properties[index];
			return flatMap(exit(pending), (exit) => {
				const state = {
					...snapshot,
					out: { ...snapshot.out }
				};
				const terminal = stepProperty(state, property, exit);
				if (terminal) return terminal;
				const done = () => succeed(state.out);
				const eff = parseProperties(state, properties.slice(index + 1));
				return eff ? flatMapEager(eff, done) : done();
			});
		};
		return (input, options) => {
			if (input === missing) return missingExit;
			if (options.errors === "all" || options.onExcessProperty !== void 0 || options.concurrency !== void 0 && resolveConcurrency(options.concurrency) !== 1) return (fallback ??= makeFallback())(input, options);
			if (!(typeof input === "object" && input !== null && !Array.isArray(input))) return fail$1(new InvalidType(ast, input, options));
			const props = compileMembers();
			const record = input;
			const out = {};
			const state = {
				ast,
				input: record,
				out,
				issues: void 0,
				options
			};
			try {
				for (let index = 0; index < props.length; index++) {
					const property = props[index];
					const name = property.name;
					const hasKey = hasPropertySignature(record, name);
					const value = hasKey ? record[name] : missing;
					const exit = property.parser(value, options);
					if (!effectIsExit(exit)) return resume(state, index, exit);
					if (exit === sameExit) {
						if (hasKey) assignProperty(out, name, value);
						continue;
					}
					const terminal = stepProperty(state, property, exit);
					if (terminal) return terminal;
				}
			} catch (error) {
				return die(error);
			}
			return succeed(out);
		};
	}
	_rebuild(recur, recurParameter, checks, encodingChecks) {
		const props = mapOrSame(this.propertySignatures, (ps) => {
			const t = recur(ps.type);
			return t === ps.type ? ps : new PropertySignature(ps.name, t);
		});
		const indexes = mapOrSame(this.indexSignatures, (is) => {
			const p = recurParameter(is.parameter);
			const t = recur(is.type);
			return p === is.parameter && t === is.type ? is : new IndexSignature(p, t);
		});
		return props === this.propertySignatures && indexes === this.indexSignatures && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Objects(props, indexes, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	recur(recur, recurParameter = recur) {
		return this._rebuild(recur, recurParameter, this.checks, this.encodingChecks);
	}
	/** @internal */
	getExpected() {
		if (this.propertySignatures.length === 0 && this.indexSignatures.length === 0) return "object | array";
		return "object";
	}
};
/** @internal */
function stepProperty(s, p, exit) {
	if (exit._tag === "Failure") return wrapPropertyKeyIssue(s, s.ast, p.name, exit);
	if (exit === sameExit) return;
	const value = exit[args];
	if (value !== missing) {
		assignProperty(s.out, p.name, value);
		return;
	}
	delete s.out[p.name];
	if (!isOptional(p.type)) {
		const issue = new Pointer([p.name], new MissingKey(p.type.context?.annotations));
		if (s.options.errors === "all") {
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
			return;
		} else return fail$2(new Composite(s.ast, [issue], s.input, s.options));
	}
}
const parsePropertiesOptions = {
	onItem(s, p) {
		if (!hasPropertySignature(s.input, p.name)) return p.parser(missing, s.options);
		const value = s.input[p.name];
		assignProperty(s.out, p.name, value);
		return p.parser(value, s.options);
	},
	step: stepProperty
};
/** @internal */
const parseProperties = /*#__PURE__*/ iterateEager()(parsePropertiesOptions);
const parsePropertiesConcurrent = /*#__PURE__*/ iterateConcurrent()(parsePropertiesOptions);
function combineChecks(a, b) {
	if (!a) return b;
	if (!b) return a;
	return [...a, ...b];
}
/** @internal */
function struct(fields, checks, annotations) {
	return new Objects(Reflect.ownKeys(fields).map((key) => {
		return new PropertySignature(key, fields[key].ast);
	}), [], annotations, checks);
}
const toCandidate = /*#__PURE__*/ memoizeIdempotent((ast) => {
	while (true) {
		if (isSuspend(ast)) return unknown;
		const encoding = ast.encoding;
		if (!encoding) return ast.recur?.(toCandidate, identity) ?? ast;
		if (encoding.some((link) => link.transformation._tag === "Middleware" && link.transformation.decode !== identity)) return unknown;
		ast = encoding[encoding.length - 1].to;
	}
});
function getCandidateTypes(ast) {
	switch (ast._tag) {
		case "Null": return ["null"];
		case "Undefined": return ["undefined"];
		case "String":
		case "TemplateLiteral": return ["string"];
		case "Number": return ["number"];
		case "Boolean": return ["boolean"];
		case "Symbol":
		case "UniqueSymbol": return ["symbol"];
		case "BigInt": return ["bigint"];
		case "Arrays": return ["array"];
		case "ObjectKeyword": return [
			"object",
			"array",
			"function"
		];
		case "Objects": return ast.propertySignatures.length || ast.indexSignatures.length ? ["object"] : [
			"string",
			"number",
			"boolean",
			"symbol",
			"bigint",
			"object",
			"array",
			"function"
		];
		case "Enum": return Array.from(new Set(ast.enums.map(([, v]) => typeof v)));
		case "Literal": return [typeof ast.literal];
		case "Union": return Array.from(new Set(ast.types.flatMap(getCandidateTypes)));
		default: return [
			"null",
			"undefined",
			"string",
			"number",
			"boolean",
			"symbol",
			"bigint",
			"object",
			"array",
			"function"
		];
	}
}
/** @internal */
function collectSentinels(ast) {
	switch (ast._tag) {
		default: return [];
		case "Declaration": {
			const s = ast.annotations?.[SENTINELS_ANNOTATION_KEY];
			return Array.isArray(s) ? s : [];
		}
		case "Objects": return ast.propertySignatures.flatMap((ps) => {
			const type = ps.type;
			if (!isOptional(type)) {
				if (isLiteral(type)) return [{
					key: ps.name,
					literal: type.literal
				}];
				if (isUniqueSymbol(type)) return [{
					key: ps.name,
					literal: type.symbol
				}];
			}
			return [];
		});
		case "Arrays": return ast.elements.flatMap((e, i) => {
			if (!isOptional(e)) {
				if (isLiteral(e)) return [{
					key: i,
					literal: e.literal
				}];
				if (isUniqueSymbol(e)) return [{
					key: i,
					literal: e.symbol
				}];
			}
			return [];
		});
		case "Union": {
			if (ast.types.length === 0) return [];
			const members = ast.types.map((type) => collectSentinels(toCandidate(type)));
			return members[0].filter((s) => members.every((sentinels) => sentinels.some((o) => o.key === s.key && o.literal === s.literal)));
		}
		case "Suspend": return collectSentinels(ast.thunk());
	}
}
const candidateIndexCache = /*#__PURE__*/ new WeakMap();
const emptyCandidates = /*#__PURE__*/ Object.freeze([]);
const getRuntimeType = (input) => input === null ? "null" : Array.isArray(input) ? "array" : typeof input;
const hasPropertySignature = (input, key) => key === "__proto__" ? Object.hasOwn(input, key) : key in input;
/** @internal */
function getCandidateIndex(types) {
	let index = candidateIndexCache.get(types);
	if (index) return index;
	let bySentinel;
	let sentinelCandidateCount = 0;
	let otherwise;
	let literalCandidates;
	let onlyLiterals = true;
	const literalOf = [];
	for (let i = 0; i < types.length; i++) {
		const a = types[i];
		const encoded = toCandidate(a);
		if (isNever(encoded)) continue;
		if (isLiteral(encoded) || isUniqueSymbol(encoded)) {
			literalCandidates ??= /* @__PURE__ */ new Map();
			const literal = isLiteral(encoded) ? encoded.literal : encoded.symbol;
			literalOf[i] = literal;
			let arr = literalCandidates.get(literal);
			if (!arr) literalCandidates.set(literal, arr = []);
			arr.push(i);
		} else onlyLiterals = false;
		const sentinels = collectSentinels(encoded);
		if (sentinels.length) {
			bySentinel ??= /* @__PURE__ */ new Map();
			sentinelCandidateCount++;
			for (const { key, literal } of sentinels) {
				let entry = bySentinel.get(key);
				if (!entry) bySentinel.set(key, entry = [/* @__PURE__ */ new Map(), /* @__PURE__ */ new Set()]);
				entry[1].add(i);
				let indexes = entry[0].get(literal);
				if (!indexes) entry[0].set(literal, indexes = /* @__PURE__ */ new Set());
				indexes.add(i);
			}
		} else {
			otherwise ??= {};
			const candidateTypes = getCandidateTypes(encoded);
			for (const t of candidateTypes) (otherwise[t] ??= []).push(i);
		}
	}
	const fallbacks = {};
	const getFallback = (type) => fallbacks[type] ??= Object.freeze(otherwise?.[type] ?? emptyCandidates);
	if (onlyLiterals && literalCandidates) {
		literalCandidates.forEach(Object.freeze);
		index = (input) => literalCandidates.get(input) ?? emptyCandidates;
	} else if (bySentinel?.size === 1 && !otherwise) {
		const [key, [byValue]] = bySentinel.entries().next().value;
		const candidates = /* @__PURE__ */ new Map();
		for (const [literal, indexes] of byValue) candidates.set(literal, Object.freeze(Array.from(indexes)));
		const all = Object.freeze(types.map((_, i) => i));
		index = (input, isConstructor) => {
			if (isObjectKeyword(input)) {
				const value = hasPropertySignature(input, key) ? input[key] : void 0;
				if (value !== void 0) return candidates.get(value) ?? emptyCandidates;
				if (isConstructor) return all;
			}
			return emptyCandidates;
		};
	} else if (bySentinel) {
		let commonSentinel;
		for (const entry of bySentinel) if ((!commonSentinel || entry[1][0].size > commonSentinel[1][0].size) && entry[1][1].size === sentinelCandidateCount) commonSentinel = entry;
		index = (input, isConstructor) => {
			const runtimeType = getRuntimeType(input);
			if (!isObjectKeyword(input)) return getFallback(runtimeType);
			const selected = new Set(otherwise?.[runtimeType]);
			let directKey;
			if (commonSentinel) {
				const [key, [byValue]] = commonSentinel;
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					if (!match) return getFallback(runtimeType);
					for (const i of match) selected.add(i);
					directKey = key;
				}
			}
			if (directKey === void 0) for (const [key, [byValue, all]] of bySentinel) {
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					if (match) for (const i of match) selected.add(i);
				} else if (isConstructor) for (const i of all) selected.add(i);
			}
			for (const [key, [byValue, all]] of bySentinel) {
				if (key === directKey) continue;
				const hasKey = hasPropertySignature(input, key);
				const value = hasKey ? input[key] : void 0;
				if (hasKey && (!isConstructor || value !== void 0)) {
					const match = byValue.get(value);
					for (const i of selected) if (all.has(i) && !match?.has(i)) selected.delete(i);
				}
			}
			return Array.from(selected).sort((a, b) => a - b);
		};
	} else index = (input) => {
		const fallback = getFallback(getRuntimeType(input));
		return literalCandidates ? fallback.filter((i) => literalOf[i] === void 0 || literalOf[i] === input) : fallback;
	};
	candidateIndexCache.set(types, index);
	return index;
}
/**
* Constructs a {@link Union}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Union$1 = class extends ASTNodeImpl {
	_tag = "Union";
	types;
	options;
	encodingChecks;
	constructor(types, options, annotations, checks, encoding, context, encodingChecks) {
		super(annotations, checks, encoding, context);
		this.types = types;
		this.options = options;
		this.encodingChecks = encodingChecks;
	}
	/** @internal */
	getParser(compile, compileField) {
		const ast = this;
		const isConstructor = compileField !== void 0;
		const parsers = [];
		const parser = (i) => parsers[i] ??= compile(ast.types[i]);
		let index;
		return (input, options) => {
			if (input === missing) return missingExit;
			const candidates = (index ??= getCandidateIndex(ast.types))(input, isConstructor);
			if (candidates.length === 0) return fail$1(new AnyOf(ast, [], input, options));
			if (candidates.length === 1) {
				const result = parser(candidates[0])(input, options);
				if (result._tag === "Success") return result;
				return effectIsExit(result) ? failSingleUnionCandidate(ast, result.cause, input, options) : catchSingleUnionCandidate(ast, result, input, options);
			}
			return parseUnionCandidates(ast, parser, candidates, input, options);
		};
	}
	_rebuild(recur, checks, encodingChecks) {
		const types = mapOrSame(this.types, recur);
		return types === this.types && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Union$1(types, this.options, this.annotations, checks, void 0, this.context, encodingChecks);
	}
	/** @internal */
	recur(recur) {
		return this._rebuild(recur, this.checks, this.encodingChecks);
	}
	/** @internal */
	flip(recur) {
		return this._rebuild(recur, this.encodingChecks, this.checks);
	}
	/** @internal */
	matchPart(s, options) {
		for (const type of this.types) {
			const out = type.matchPart(s, options);
			if (out !== void 0) return out;
		}
	}
	/** @internal */
	getExpected(getExpected) {
		const expected = this.annotations?.expected;
		if (typeof expected === "string") return expected;
		if (this.types.length === 0) return "never";
		const types = this.types.map((type) => {
			const encoded = toEncoded(type);
			switch (encoded._tag) {
				case "Arrays": {
					const literals = encoded.elements.filter(isLiteral);
					if (literals.length > 0) return `${formatIsMutable(encoded.isMutable)}[ ${literals.map((e) => getExpected(e) + formatIsOptional(e.context?.isOptional)).join(", ")}, ... ]`;
					break;
				}
				case "Objects": {
					const literals = encoded.propertySignatures.filter((ps) => isLiteral(ps.type));
					if (literals.length > 0) return `{ ${literals.map((ps) => `${formatIsMutable(ps.type.context?.isMutable)}${formatPropertyKey(ps.name)}${formatIsOptional(ps.type.context?.isOptional)}: ${getExpected(ps.type)}`).join(", ")}, ... }`;
					break;
				}
			}
			return getExpected(encoded);
		});
		return Array.from(new Set(types)).join(" | ");
	}
};
function failSingleUnionCandidate(ast, cause, input, options) {
	const issue = getSchemaIssue(cause);
	if (!issue) return failCause$2(cause);
	return fail$2(new AnyOf(ast, [issue], input, options));
}
function catchSingleUnionCandidate(ast, result, input, options) {
	return catchCause(result, (cause) => failSingleUnionCandidate(ast, cause, input, options));
}
function parseUnionCandidates(ast, parser, candidates, input, options) {
	const state = {
		ast,
		parser,
		input,
		out: void 0,
		successes: ast.options?.mode === "oneOf" ? [] : void 0,
		issues: void 0,
		options
	};
	const eff = parseUnion(state, candidates);
	if (!eff) {
		if (state.out) return state.out;
		return fail$1(new AnyOf(ast, state.issues ?? [], input, options));
	}
	if (effectIsExit(eff)) return resumeUnion(eff, state);
	let first = true;
	return suspend(() => {
		if (!first) return parseUnionCandidates(ast, parser, candidates, input, options);
		first = false;
		return resumeUnion(eff, state);
	});
}
function resumeUnion(eff, state) {
	return flatMapEager(eff, (_) => {
		if (state.out === sameExit) return succeed$1(state.input);
		if (state.out) return state.out;
		return fail$1(new AnyOf(state.ast, state.issues ?? [], state.input, state.options));
	});
}
const parseUnion = /*#__PURE__*/ iterateEager()({
	onItem(s, i) {
		return s.parser(i)(s.input, s.options);
	},
	step(s, i, exit) {
		if (exit._tag === "Failure") {
			const issue = getSchemaIssue(exit.cause);
			if (issue === void 0) return exit;
			if (s.issues) s.issues.push(issue);
			else s.issues = [issue];
		} else {
			if (s.out && s.successes) {
				s.successes.push(s.ast.types[i]);
				return fail$2(new OneOf(s.ast, s.successes, s.input, s.options));
			}
			s.out = exit;
			if (s.successes) s.successes.push(s.ast.types[i]);
			else return void_$1;
		}
	}
});
const nonFiniteLiterals = /*#__PURE__*/ new Union$1([
	/*#__PURE__*/ new Literal$1("Infinity"),
	/*#__PURE__*/ new Literal$1("-Infinity"),
	/*#__PURE__*/ new Literal$1("NaN")
]);
function formatIsMutable(isMutable) {
	return isMutable ? "" : "readonly ";
}
function formatIsOptional(isOptional) {
	return isOptional ? "?" : "";
}
/**
* Constructs a {@link Filter}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Filter = class extends Class$1 {
	_tag = "Filter";
	run;
	annotations;
	/**
	* Whether the parsing process should be aborted after this check has failed.
	*/
	aborted;
	constructor(run, annotations = void 0, aborted = false) {
		super();
		this.run = run;
		this.annotations = annotations;
		this.aborted = aborted;
	}
	annotate(annotations) {
		return new Filter(this.run, {
			...this.annotations,
			...annotations
		}, this.aborted);
	}
	abort() {
		return new Filter(this.run, this.annotations, true);
	}
	and(other, annotations) {
		return new FilterGroup([this, other], annotations);
	}
};
/**
* Constructs a {@link FilterGroup}.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const FilterGroup = class extends Class$1 {
	_tag = "FilterGroup";
	checks;
	annotations;
	constructor(checks, annotations = void 0) {
		super();
		this.checks = checks;
		this.annotations = annotations;
	}
	annotate(annotations) {
		return new FilterGroup(this.checks, {
			...this.annotations,
			...annotations
		});
	}
	and(other, annotations) {
		return new FilterGroup([this, other], annotations);
	}
};
/** @internal */
function makeFilter$1(filter, annotations, aborted = false) {
	return new Filter((input, ast, options) => normalizeFilterOutput(ast, filter(input, ast, options), input, options), annotations, aborted);
}
/** @internal */
function isFinite(annotations) {
	return makeFilter$1((n) => globalThis.Number.isFinite(n), {
		expected: "a finite number",
		representation: {
			id: "effect/schema/isFinite",
			payload: null
		},
		toJsonSchema: () => ({ type: "number" }),
		toCode: () => ({ runtime: "Schema.isFinite()" }),
		arbitraryConstraint: { number: "finite" },
		...annotations
	});
}
const numberToJson = /*#__PURE__*/ new Link(/*#__PURE__*/ new Union$1([/* @__PURE__ */ appendChecks(number, [/*#__PURE__*/ isFinite()]), nonFiniteLiterals]), /*#__PURE__*/ new Transformation(/*#__PURE__*/ Number$3(), /*#__PURE__*/ transform$1((n) => globalThis.Number.isFinite(n) ? n : globalThis.String(n))));
/**
* Creates a {@link Filter} that validates strings by running `RegExp.test`.
*
* **When to use**
*
* Use when string validation should be represented as a schema `Filter` backed
* by a regular expression.
*
* **Details**
*
* The filter can be used with `Schema.check` or attached directly to a
* `String` AST node through checks. The regular expression is cloned and its
* `lastIndex` is reset before each test, so global and sticky expressions are
* deterministic and the provided regular expression is not mutated. The
* regular expression source is stored in annotations for serialization and
* arbitrary generation.
*
* **Gotchas**
*
* Arbitrary metadata preserves both `regExp.source` and `regExp.flags`.
* Implementations that cannot consume all flags may still use the source as a
* generation hint because the Schema filter validates every generated value.
* JSON Schema has no way to carry JavaScript regular-expression flags. Unless
* annotations provide `toJsonSchema`, the RegExp constraint is omitted from
* JSON Schema export.
*
* **Example** (Validating an email pattern)
*
* ```ts import.meta.vitest
* import { SchemaAST } from "effect"
*
* const emailFilter = SchemaAST.isPattern(/^[^@]+@[^@]+$/)
* emailFilter.run("alice@example.com", SchemaAST.string, {}) // => undefined
* emailFilter.run("invalid", SchemaAST.string, {})?._tag // => "InvalidValue"
* ```
*
* @see {@link Filter}
* @stability stable
* @category constructors
* @since 4.0.0
*/
function isPattern(regExp, annotations) {
	const copy = new globalThis.RegExp(regExp);
	const payload = {
		source: copy.source,
		flags: copy.flags
	};
	return makeFilter$1((s) => {
		copy.lastIndex = 0;
		return copy.test(s);
	}, {
		expected: `a string matching the RegExp ${payload.source}`,
		representation: {
			id: "effect/schema/isPattern",
			payload
		},
		toJsonSchema: () => [{}, true],
		arbitraryConstraint: { patterns: [payload] },
		...annotations
	});
}
const bodyOwners = /*#__PURE__*/ new WeakMap();
function copy(ast, changes) {
	const out = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, changes);
	if (Reflect.ownKeys(changes).every((key) => key === "context" || key === "encoding")) bodyOwners.set(out, getContextOwner(ast));
	return out;
}
/** @internal */
function getContextOwner(ast) {
	const existing = bodyOwners.get(ast);
	if (existing !== void 0) return existing;
	if (ast.encoding === void 0) return ast;
	const owner = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, { encoding: void 0 });
	bodyOwners.set(ast, owner);
	return owner;
}
/** @internal */
function replaceEncoding(ast, encoding) {
	return ast.encoding === encoding ? ast : copy(ast, { encoding });
}
/** @internal */
function replaceContext(ast, context) {
	if (ast.context === context) return ast;
	const owner = getContextOwner(ast);
	if (owner.context === context && owner.encoding === ast.encoding) return owner;
	return copy(ast, { context });
}
/** @internal */
function getLastEncoding(ast) {
	return ast.encoding ? getLastEncoding(ast.encoding[ast.encoding.length - 1].to) : ast;
}
/** @internal */
function annotate(ast, annotations) {
	if (ast.checks) {
		const last = ast.checks[ast.checks.length - 1];
		return replaceChecks(ast, append$1(ast.checks.slice(0, -1), last.annotate(annotations)));
	}
	return copy(ast, { annotations: {
		...ast.annotations,
		...annotations
	} });
}
/** @internal */
function replaceChecks(ast, checks) {
	if (ast._tag === "Suspend" && checks) throw new Error("Cannot add checks to Suspend");
	if (ast.checks === checks) return ast;
	return copy(ast, { checks });
}
/** @internal */
function appendChecks(ast, checks) {
	return replaceChecks(ast, combineChecks(ast.checks, checks));
}
/** @internal */
function mapLink(link, f) {
	const to = f(link.to);
	return to === link.to ? link : new Link(to, link.transformation);
}
function updateLastLink(encoding, f) {
	const links = encoding;
	const last = links[links.length - 1];
	const out = mapLink(last, f);
	return out === last ? encoding : append$1(encoding.slice(0, encoding.length - 1), out);
}
/** @internal */
function applyToSelfOrLastLinkEncodingIdempotent(f, options) {
	function out(ast) {
		if (ast.encoding) {
			const last = ast.encoding[ast.encoding.length - 1];
			return options?.stopAt?.(last) ? ast : replaceEncoding(ast, updateLastLink(ast.encoding, out));
		}
		return f(ast);
	}
	return memoizeIdempotent(out);
}
function appendTransformation(from, transformation, to) {
	const link = new Link(from, transformation);
	return replaceEncoding(to, to.encoding ? [...to.encoding, link] : [link]);
}
function mapOrSame(as, f) {
	let out;
	for (let i = 0; i < as.length; i++) {
		const a = as[i];
		const fa = f(a);
		if (out) out[i] = fa;
		else if (fa !== a) {
			out = new Array(as.length);
			for (let j = 0; j < i; j++) out[j] = as[j];
			out[i] = fa;
		}
	}
	return out ?? as;
}
/** @internal */
function annotateKey(ast, annotations) {
	return replaceContext(ast, ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, ast.context.constructorDefault, {
		...ast.context.annotations,
		...annotations
	}) : new Context(false, false, void 0, annotations));
}
/** @internal */
function withConstructorDefault$1(ast, defaultValue) {
	return replaceContext(ast, ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, defaultValue, ast.context.annotations) : new Context(false, false, defaultValue));
}
/**
* Attaches a `Transformation` to the `to` AST, making it decode from the
* `from` AST and encode back to it.
*
* **Details**
*
* This is the low-level primitive behind `Schema.decodeTo`. It appends a
* {@link Link} to the `to` node's encoding chain.
*
* - Returns a new AST with the same type as `to`.
*
* @see {@link Link}
* @see {@link Encoding}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
function decodeTo$1(from, to, transformation) {
	return appendTransformation(from, transformation, to);
}
/**
* Returns `true` if the AST node represents an optional property.
*
* **Details**
*
* Checks `ast.context?.isOptional`. Defaults to `false` when no
* {@link Context} is set.
*
* @see `Schema.optionalKey`
* @see {@link Context}
* @stability stable
* @category predicates
* @since 4.0.0
*/
function isOptional(ast) {
	return ast.context?.isOptional ?? false;
}
function isStructuralCheck(check) {
	return check.annotations?.["~structural"] === true || check._tag === "FilterGroup" && check.checks.every(isStructuralCheck);
}
function extractStructuralChecks(checks) {
	function extract(check) {
		if (isStructuralCheck(check)) return [check];
		return check._tag === "FilterGroup" ? check.checks.flatMap(extract) : [];
	}
	const out = checks.flatMap(extract);
	return isArrayNonEmpty(out) ? out : void 0;
}
function canPreserveEncodingChecks(ast) {
	let preserve = true;
	function visit(child) {
		preserve = preserve && !child.encoding && !isSuspend(child);
		if (preserve && "recur" in child) child.recur(visit);
		return child;
	}
	if ("recur" in ast) ast.recur(visit);
	return preserve;
}
/**
* Strips all encoding transformations from an AST, returning the decoded
* (type-level) representation.
*
* **Details**
*
* - Memoized: same input reference → same output reference.
* - Recursively walks into composite nodes ({@link Arrays}, {@link Objects},
*   {@link Union}, {@link Suspend}).
*
* **Example** (Getting the type AST)
*
* ```ts import.meta.vitest
* import { Schema, SchemaAST } from "effect"
*
* const schema = Schema.NumberFromString
* const typeAst = SchemaAST.toType(schema.ast)
* typeAst._tag // => "Number"
* ```
*
* @see {@link toEncoded}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const toType = /*#__PURE__*/ memoizeIdempotent((ast) => {
	const owner = getContextOwner(ast);
	if (owner !== ast) {
		const type = toType(owner);
		return type === owner && ast.encoding === void 0 ? ast : replaceContext(type, ast.context);
	}
	const type = "recur" in ast ? ast.recur(toType) : ast;
	if ("encodingChecks" in type && type.encodingChecks) {
		const checks = canPreserveEncodingChecks(ast) ? type.encodingChecks : isArrays(type) || isObjects(type) || isDeclaration(type) && type.typeParameters.length > 0 ? extractStructuralChecks(type.encodingChecks) : void 0;
		return copy(type, {
			encodingChecks: void 0,
			checks: combineChecks(type.checks, checks)
		});
	}
	return type;
});
/**
* Returns the encoded (wire-format) AST by flipping and then stripping
* encodings.
*
* **Details**
*
* Equivalent to `toType(flip(ast))`. This gives you the AST that describes
* the shape of the serialized/encoded data.
*
* - Memoized: same input reference → same output reference.
*
* **Example** (Getting the encoded AST)
*
* ```ts import.meta.vitest
* import { Schema, SchemaAST } from "effect"
*
* const schema = Schema.NumberFromString
* const encodedAst = SchemaAST.toEncoded(schema.ast)
* encodedAst._tag // => "String"
* ```
*
* @see {@link toType}
* @see {@link flip}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const toEncoded = /*#__PURE__*/ memoizeIdempotent((ast) => {
	return toType(flip(ast));
});
function flipEncoding(ast, encoding) {
	const links = encoding;
	const len = links.length;
	const last = links[len - 1];
	const ls = [new Link(flip(replaceEncoding(ast, void 0)), links[0].transformation.flip())];
	for (let i = 1; i < len; i++) ls.unshift(new Link(flip(links[i - 1].to), links[i].transformation.flip()));
	const to = flip(last.to);
	if (to.encoding) return replaceEncoding(to, [...to.encoding, ...ls]);
	else return replaceEncoding(to, ls);
}
/**
* Swaps the decode and encode directions of an AST's {@link Encoding} chain.
*
* **Details**
*
* After flipping, what was decoding becomes encoding and vice versa. This is
* the core operation behind `Schema.encodeEffect` — encoding a value is decoding
* with a flipped SchemaAST.
*
* - Memoized: same input reference → same output reference.
* - Recursively walks composite nodes.
*
* @see {@link toType}
* @see {@link toEncoded}
* @stability stable
* @category transforming
* @since 4.0.0
*/
const flip = /*#__PURE__*/ memoize((ast) => {
	if (ast.encoding) return flipEncoding(ast, ast.encoding);
	const owner = getContextOwner(ast);
	if (owner !== ast) {
		const flipped = flip(owner);
		return flipped === owner ? ast : replaceContext(flipped, ast.context);
	}
	return "flip" in ast ? ast.flip(flip) : "recur" in ast ? ast.recur(flip) : ast;
});
function fromConst(ast, value) {
	const succeed$7 = value === 0 ? sameExit : succeed(value);
	return (input, options) => {
		if (input === missing) return missingExit;
		if (input === value) return succeed$7;
		return fail$1(new InvalidType(ast, input, options));
	};
}
function fromRefinement(ast, refinement) {
	return (input, options) => {
		if (input === missing) return missingExit;
		if (refinement(input)) return sameExit;
		return fail$1(new InvalidType(ast, input, options));
	};
}
/** @internal */
const parameterFromPropertyKey = /*#__PURE__*/ applyToSelfOrLastLinkEncodingIdempotent((ast) => {
	switch (ast._tag) {
		default: return ast;
		case "Number": return ast.toCodecStringTree();
		case "Union": return ast.recur(parameterFromPropertyKey);
	}
});
const isStringFiniteRegExp = /*#__PURE__*/ new globalThis.RegExp(`^${FINITE_PATTERN}$`);
const isStringNumberRegExp = /*#__PURE__*/ new globalThis.RegExp(`^(?:${FINITE_PATTERN}|Infinity|-Infinity|NaN)$`);
/** @internal */
function isStringFinite(annotations) {
	return isPattern(isStringFiniteRegExp, {
		expected: "a string representing a finite number",
		representation: {
			id: "effect/schema/isStringFinite",
			payload: null
		},
		toJsonSchema: () => ({ pattern: isStringFiniteRegExp.source }),
		...annotations
	});
}
const finiteString = /*#__PURE__*/ appendChecks(string, [/*#__PURE__*/ isStringFinite()]);
const finiteToString = /*#__PURE__*/ new Link(finiteString, numberFromString);
const numberToString = /*#__PURE__*/ new Link(/*#__PURE__*/ new Union$1([finiteString, nonFiniteLiterals]), numberFromString);
/** @internal */
function collectIssues(checks, value, issues, ast, options) {
	for (let i = 0; i < checks.length; i++) {
		const check = checks[i];
		if (check._tag === "FilterGroup") {
			issues = collectIssues(check.checks, value, issues, ast, options);
			if (issues && (options.errors !== "all" || issues[issues.length - 1].filter.aborted)) return issues;
		} else {
			const issue = check.run(value, ast, options);
			if (issue) {
				const filter = new Filter$1(check, issue, value, options);
				if (issues) issues.push(filter);
				else issues = [filter];
				if (options.errors !== "all" || check.aborted) return issues;
			}
		}
	}
	return issues;
}
/** @internal */
function getConstructorDescriptor(ast) {
	if (!isDeclaration(ast)) return void 0;
	const getDescriptor = ast.annotations?.[CONSTRUCTOR_ANNOTATION_KEY];
	return isFunction(getDescriptor) ? getDescriptor(ast.typeParameters) : void 0;
}
function isJsonLeaf(u) {
	return u === null || typeof u === "string" || typeof u === "boolean" || typeof u === "number" && globalThis.Number.isFinite(u);
}
function isStringTreeLeaf(u) {
	return u === void 0 || typeof u === "string";
}
function isTree(u, isLeaf) {
	const cache = /* @__PURE__ */ new WeakMap();
	const stack = [];
	outer: while (true) {
		if (typeof u !== "object" || u === null) {
			if (!isLeaf(u)) return false;
		} else {
			const value = u;
			const cached = cache.get(value);
			if (cached === false) return false;
			if (cached === void 0) {
				const isArray = Array.isArray(value);
				if (!isArray) {
					const prototype = Object.getPrototypeOf(value);
					if (prototype !== null && prototype !== Object.prototype && Object.getPrototypeOf(prototype) !== null) return false;
				}
				cache.set(value, false);
				stack.push({
					value,
					keys: isArray ? value.length : Object.keys(value),
					index: 0
				});
			}
		}
		while (stack.length > 0) {
			const frame = stack[stack.length - 1];
			const keys = frame.keys;
			if (typeof keys === "number") {
				if (frame.index < keys) {
					u = frame.value[frame.index++];
					continue outer;
				}
			} else if (frame.index < keys.length) {
				u = frame.value[keys[frame.index++]];
				continue outer;
			}
			cache.set(frame.value, true);
			stack.pop();
		}
		return true;
	}
}
/**
* Returns true if the value is a JSON value.
*
* When a cyclic reference is detected, returns false.
*
* @internal
*/
function isJson(u) {
	return isTree(u, isJsonLeaf);
}
/** @internal */
const Json$1 = /*#__PURE__*/ new Declaration([], () => (input, ast, options) => isJson(input) ? sameExit : fail$1(new InvalidType(ast, input, options)), {
	representation: {
		id: "effect/schema/Json",
		payload: null
	},
	expected: "JSON value",
	toCodecJson: () => void 0,
	toCodecStringTree: () => unknownToStringTree
});
/**
* Returns true if the value is a StringTree value.
*
* When a cyclic reference is detected, returns false.
*
* @internal
*/
function isStringTree(u) {
	return isTree(u, isStringTreeLeaf);
}
/** @internal */
const unknownToStringTree = /*#__PURE__*/ new Link(/* @__PURE__ */ new Declaration([], () => (input, ast, options) => isStringTree(input) ? sameExit : fail$1(new InvalidType(ast, input, options)), {
	expected: "StringTree",
	toCodecStringTree: () => void 0
}), /*#__PURE__*/ passthrough());
// node_modules/effect/dist/internal/schema/interpreter.js
const flatMapTransformation = (result, current, f) => result === sameExit ? f(current) : flatMapEager(result, f);
function compileTransformation(transformation) {
	const apply = compileTransformationUnsafe(transformation);
	return (result, current, options) => {
		try {
			return apply(result, current, options);
		} catch (error) {
			return die(error);
		}
	};
}
function compileTransformationUnsafe(transformation) {
	if (transformation._tag === "Middleware") return (result, current, options) => {
		const transformed = result === sameExit ? transformation.decode(succeed(toOption(current)), options) : transformation.decode(mapEager(result, toOption), options);
		return fromOptionalEffect(transformed);
	};
	const getter = transformation.decode;
	switch (getter._tag) {
		case "Passthrough": return (result, current) => result === sameExit ? succeed(current) : result;
		case "Transform": {
			const transform = (value) => value === missing ? missingExit : succeed(getter.transform(value));
			return (result, current) => flatMapTransformation(result, current, transform);
		}
		case "TransformOptional": {
			const transform = (value) => fromOptionExit(getter.transform(toOption(value)));
			return (result, current) => flatMapTransformation(result, current, transform);
		}
		case "TransformEffect": return (result, current, options) => flatMapTransformation(result, current, (value) => value === missing ? missingExit : getter.transform(value, options));
		case "TransformOptionalEffect": return (result, current, options) => flatMapTransformation(result, current, (value) => fromOptionalEffect(getter.transform(toOption(value), options)));
	}
}
const fromOptionalEffect = (effect) => flatMapEager(effect, fromOptionExit);
/** @internal */
const wrapEncoding = (ast, input, options, effect) => catchCause(effect, (cause) => failCauseSync(() => map$1(cause, (issue) => new Encoding(ast, issue, input, options))));
function makeConstructorParser(descriptor, compile) {
	const transform = compileTransformation(descriptor.link.transformation);
	let sourceParser;
	return (input, options) => {
		if (input === missing) return missingExit;
		if (descriptor.isConstructed(input)) return sameExit;
		const result = (sourceParser ??= compile(descriptor.link.to))(input, options);
		return transform(result, input, options);
	};
}
function withDefault(ast, parser) {
	const defaultValue = ast.context.constructorDefault;
	return (input, options) => {
		if (input !== missing && input !== void 0) return parser(input, options);
		const result = defaultValue;
		if (effectIsExit(result) && result._tag === "Success") {
			const local = parser(result[args], options);
			return local === sameExit ? result : local;
		}
		return flatMapEager(wrapEncoding(ast, input, options, result), (value) => {
			const local = parser(value, options);
			return local === sameExit ? succeed(value) : local;
		});
	};
}
/** @internal */
function compileField(ast, compile) {
	const parser = compile(ast);
	return ast.context?.constructorDefault === void 0 ? parser : withDefault(ast, parser);
}
/** @internal */
function compile(ast, compile, compileField, base, specialize) {
	if (ast._tag === "Declaration") for (const parameter of ast.typeParameters) compile(parameter);
	const descriptor = compileField ? getConstructorDescriptor(ast) : void 0;
	const parser = descriptor ? makeConstructorParser(descriptor, compile) : base ?? ast.getParser(compile, compileField);
	const checks = ast.checks;
	const links = ast.encoding;
	const transformations = links?.map((link) => compileTransformation(link.transformation));
	const encodingChecks = ast.encodingChecks;
	if (!links && !checks && !encodingChecks) return parser;
	let encodingParsers;
	const parseChecks = (input, options) => {
		let result = parser(input, options);
		try {
			if (encodingChecks && !options.disableChecks) {
				if (effectIsExit(result)) {
					if (result._tag === "Success") {
						const output = result === sameExit ? input : result[args];
						if (input !== missing && output !== missing) {
							const issues = collectIssues(encodingChecks, input, void 0, ast, options);
							if (issues) result = fail$1(new Composite(ast, issues, input, options));
						}
					}
				} else result = flatMap(result, (value) => {
					if (input !== missing && value !== missing) {
						const issues = collectIssues(encodingChecks, input, void 0, ast, options);
						if (issues) return fail$1(new Composite(ast, issues, input, options));
					}
					return succeed$1(value);
				});
			}
			if (checks && !options.disableChecks) {
				if (effectIsExit(result)) {
					if (result._tag === "Success") {
						const value = result === sameExit ? input : result[args];
						if (value === missing) return result;
						const issues = collectIssues(checks, value, void 0, ast, options);
						if (issues) result = fail$1(new Composite(ast, issues, value, options));
					}
				} else result = flatMap(result, (value) => {
					if (value !== missing) {
						const issues = collectIssues(checks, value, void 0, ast, options);
						if (issues) return fail$1(new Composite(ast, issues, value, options));
					}
					return succeed$1(value);
				});
			}
			return result;
		} catch (error) {
			return die(error);
		}
	};
	const parseLocal = specialize === void 0 ? parseChecks : specialize(parseChecks);
	if (!links) return parseLocal;
	return (input, options) => {
		const parsers = encodingParsers ??= links.map((link) => compile(link.to));
		let current = input;
		let result = parsers[parsers.length - 1](input, options);
		for (let i = links.length - 1; i >= 0; i--) {
			result = transformations[i](result, current, options);
			if (i !== 0) {
				const next = parsers[i - 1];
				if (result._tag === "Success") {
					current = result[args];
					result = next(current, options);
				} else result = flatMapEager(result, (value) => {
					const nextResult = next(value, options);
					return nextResult === sameExit ? succeed(value) : nextResult;
				});
			}
		}
		if (result._tag === "Success") {
			const value = result[args];
			const local = parseLocal(value, options);
			return local === sameExit ? result : local;
		}
		result = wrapEncoding(ast, input, options, result);
		return flatMapEager(result, (value) => {
			const local = parseLocal(value, options);
			return local === sameExit ? succeed(value) : local;
		});
	};
}
// node_modules/effect/dist/internal/schema/compilerRegistry.js
/** @internal */
const invalid = /*#__PURE__*/ Symbol();
const cache = /*#__PURE__*/ new WeakMap();
const decodeChild = (ast) => resolve(ast).parser;
const makeChild = (ast) => resolve(ast).makeEffect;
const makeField = (ast) => compileField(ast, makeChild);
var InterpretedEntry = class {
	ast;
	constructor(ast) {
		this.ast = ast;
	}
	get decodeEffect() {
		return this.cachedDecodeEffect ??= compile(this.ast, decodeChild);
	}
	get parser() {
		return this.decodeEffect;
	}
	get makeEffect() {
		return this.cachedMakeEffect ??= compile(this.ast, makeChild, makeField);
	}
};
/** @internal */
function resolve(ast) {
	const cached = cache.get(ast);
	if (cached !== void 0) return cached;
	const entry = new InterpretedEntry(ast);
	cache.set(ast, entry);
	return entry;
}
// node_modules/effect/dist/SchemaParser.js
/**
* Runs schemas against real values.
*
* Schema parsers construct values from schema input, check whether a value
* matches a schema, decode encoded input, and encode decoded values back to
* their external form. This module exposes those operations through several
* result styles, including `Effect`, `Promise`, `Exit`, `Option`, `Result`, and
* synchronous functions that throw. It also contains the lower-level runner that
* walks a schema AST and reports schema failures as `SchemaIssue.Issue` values.
*
* @stability stable
* @since 4.0.0
*/
/**
* Creates an effectful maker for the schema's decoded type side.
*
* **When to use**
*
* Use to construct decoded schema values in `Effect` while preserving
* construction failures as `SchemaIssue.Issue` values in the error channel.
*
* **Details**
*
* The returned function accepts constructor input, applies constructor defaults,
* runs type-side validation unless checks are disabled, and fails with a
* `SchemaIssue.Issue` when construction fails.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function makeEffect(schema) {
	const ast = schema.ast;
	let parser;
	return (input, options) => {
		return (parser ??= runWithCompiler(constructorCompiler, toType(ast)))(input, options?.disableChecks ? options?.parseOptions ? {
			...options.parseOptions,
			disableChecks: true
		} : { disableChecks: true } : options?.parseOptions);
	};
}
/**
* Creates a synchronous maker that returns `Option.some` with the constructed
* value on success, or `Option.none` when construction fails with schema issues.
*
* **When to use**
*
* Use when you need to validate schema constructor input and only care whether
* construction succeeds, without exposing `SchemaIssue.Issue` details.
*
* **Gotchas**
*
* Only causes made entirely of schema issues are converted to `Option.none`.
* Causes that contain defects, interruptions, or asynchronous work at this
* synchronous boundary throw an `Error` whose cause is the underlying `Cause`.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function makeOption(schema) {
	const parser = makeEffect(schema);
	return (input, options) => {
		const exit = runSyncExit(parser(input, options));
		if (isSuccess(exit)) return some(exit.value);
		getSchemaIssueOrThrow(exit.cause, "Option adapter can only return none for schema issues");
		return none();
	};
}
/**
* Creates a synchronous maker for the schema's decoded type side.
*
* **When to use**
*
* Use to construct decoded schema values synchronously when invalid input
* should throw an `Error` whose cause is `SchemaIssue.Issue`.
*
* **Details**
*
* The returned function constructs a value from constructor input and throws an
* `Error` with the `SchemaIssue.Issue` in its `cause` when construction fails.
* Schema validation failures use the generic message `"Schema validation failed"`.
* Format the `cause` explicitly with `SchemaIssue.makeFormatterDefault()` when
* human-readable details are needed.
*
* **Gotchas**
*
* Causes that contain defects, interruptions, or asynchronous work at this
* synchronous boundary throw an `Error` whose cause is the underlying `Cause`,
* instead of being converted to a schema validation error.
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
function make$6(schema) {
	return makeConstructorSync(toType(schema.ast));
}
const getValue = (value) => {
	if (value === missing) return fail$1(new InvalidValue());
	return succeed$1(value);
};
function parserResult(result, input) {
	if (result === sameExit) return succeed$1(input);
	if (!effectIsExit(result)) return flatMapEager(result, getValue);
	return result["~effect/Effect/args"] === missing ? getValue(missing) : result;
}
function runWithCompiler(compiler, ast) {
	let parser;
	return (input, options) => {
		const result = (parser ??= compiler(ast))(input, options ?? defaultParseOptions);
		if (result === sameExit) return succeed$1(input);
		if (!effectIsExit(result)) return flatMapEager(result, getValue);
		return result["~effect/Effect/args"] === missing ? getValue(missing) : result;
	};
}
function runSync(effect, message) {
	const exit = runSyncExit(effect);
	if (isSuccess(exit)) return exit.value;
	const issue = getSchemaIssueOrThrow(exit.cause, message);
	throw new Error("Schema validation failed", { cause: issue });
}
const constructorDefectMessage = "Constructor adapter can only throw schema issues";
function throwConstructorDefect(error) {
	throw new Error(constructorDefectMessage, { cause: die$1(error) });
}
function makeConstructorSync(ast) {
	let entry;
	return (input, options) => constructSync(entry ??= resolve(ast), input, options);
}
function constructSync(entry, input, options) {
	const parseOptions = options?.disableChecks ? options.parseOptions ? {
		...options.parseOptions,
		disableChecks: true
	} : { disableChecks: true } : options?.parseOptions ?? defaultParseOptions;
	const make = entry.make;
	if (make !== void 0 && input !== missing) {
		let output;
		try {
			output = make(input, parseOptions);
		} catch (error) {
			throwConstructorDefect(error);
		}
		if (output !== invalid && output !== missing) return output;
	}
	return runSync(parserResult(entry.makeEffect(input, parseOptions), input), constructorDefectMessage);
}
/**
* Constructs a class instance from its type-side declaration.
*
* When the declaration has no compiled constructor, the fields are validated first
* and the instance is built outside the parser, so stacks captured by the
* class constructor keep the caller's frames.
*
* @internal
*/
function makeClassSync(ast, input, options, construct) {
	const entry = resolve(ast);
	const descriptor = getConstructorDescriptor(ast);
	if (descriptor === void 0 || entry.make !== void 0 || entry.compiledMakeEffect !== void 0) return constructSync(entry, input, options);
	if (descriptor.isConstructed(input)) return input;
	const fields = constructSync((entry.resolve ?? resolve)(descriptor.link.to), input, options);
	try {
		return construct(fields);
	} catch (error) {
		throwConstructorDefect(error);
	}
}
const constructorCompiler = (ast) => resolve(ast).makeEffect;
// node_modules/effect/dist/internal/schema/make.js
/** @internal */
const TypeId$8 = "~effect/Schema/Schema";
const RebuildOptions = /*#__PURE__*/ Symbol();
const SchemaProto = {
	[TypeId$8]: TypeId$8,
	get make() {
		const value = make$6(this);
		Object.defineProperty(this, "make", {
			value,
			enumerable: true
		});
		return value;
	},
	get makeEffect() {
		const value = makeEffect(this);
		Object.defineProperty(this, "makeEffect", {
			value,
			enumerable: true
		});
		return value;
	},
	get makeOption() {
		const value = makeOption(this);
		Object.defineProperty(this, "makeOption", {
			value,
			enumerable: true
		});
		return value;
	},
	pipe() {
		return pipeArguments(this, arguments);
	},
	annotate(annotations) {
		return this.rebuild(annotate(this.ast, annotations));
	},
	annotateKey(annotations) {
		return this.rebuild(annotateKey(this.ast, annotations));
	},
	check(...checks) {
		return this.rebuild(appendChecks(this.ast, checks));
	},
	rebuild(ast) {
		return make$5(ast, this[RebuildOptions]);
	}
};
/** @internal */
function make$5(ast, options) {
	function Schema() {}
	const self = Object.setPrototypeOf(Schema, SchemaProto);
	if (options && (Object.hasOwn(options, "name") || Object.hasOwn(options, "length") || Object.hasOwn(options, "__proto__"))) Object.defineProperties(self, Object.getOwnPropertyDescriptors({ ...options }));
	else Object.assign(self, options);
	self[RebuildOptions] = options;
	self.ast = ast;
	return self;
}
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
({ ...BaseProto });
// node_modules/effect/dist/Schema.js
const TypeId$5 = TypeId$8;
/**
* Creates a schema from an AST (Abstract Syntax Tree) node.
*
* **Details**
*
* This is the fundamental constructor for all schemas in the Effect Schema
* library. It takes an AST node and wraps it in a fully-typed schema that
* preserves all type information and provides the complete schema API.
*
* The `make` function is used internally to create all primitive schemas like
* `String`, `Number`, `Boolean`, etc., as well as more complex schemas. It's
* the bridge between the untyped AST representation and the strongly-typed
* schema.
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const make$4 = make$5;
/**
* Checks whether a value is a `Schema`.
*
* @stability stable
* @category guards
* @since 3.10.0
*/
function isSchema(u) {
	return hasProperty(u, TypeId$5) && u[TypeId$5] === TypeId$5;
}
/**
* Creates a schema for a single literal value (string, number, bigint, boolean, or null).
*
* **Example** (Defining a string literal)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const schema = Schema.Literal("hello")
* // Type: Schema.Literal<"hello">
* Schema.decodeSync(schema)("hello") // => "hello"
* ```
*
* @see {@link Literals} for a schema that represents a union of literals.
* @see {@link tag} for a schema that represents a literal value that can be
* used as a discriminator field in tagged unions and has a constructor default.
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Literal(literal) {
	const out = make$4(new Literal$1(literal), {
		literal,
		transform(to) {
			return out.pipe(decodeTo(Literal(to), {
				decode: transform$1(() => to),
				encode: transform$1(() => literal)
			}));
		}
	});
	return out;
}
/**
* Schema for the `unknown` type. Accepts any value without validation.
*
* **When to use**
*
* Use as a top schema when you need to accept any input while preserving
* TypeScript's `unknown` safety at use sites.
*
* @see {@link Any} for the `any` variant.
* @stability stable
* @category schemas
* @since 3.10.0
*/
const Unknown = /*#__PURE__*/ make$4(unknown);
/**
* Schema for `string` values. Validates that the input is `typeof` `"string"`.
*
* @stability stable
* @category schemas
* @since 4.0.0
*/
const String$1 = /*#__PURE__*/ make$4(string);
function makeStruct(ast, fields) {
	return make$4(ast, {
		fields,
		mapFields(f, options) {
			const fields = f(this.fields);
			return makeStruct(struct(fields, options?.unsafePreserveChecks ? this.ast.checks : void 0), fields);
		}
	});
}
/**
* Defines a struct schema from a map of field schemas.
*
* **Details**
*
* Each field value is a schema. Use {@link optionalKey} or {@link optional} to
* mark fields as optional, and {@link mutableKey} to mark them as mutable.
*
* The resulting schema's `Type` is a readonly object type with the fields'
* decoded types. The `Encoded` form mirrors the field schemas' encoded types.
* Declared fields may be inherited and are copied to own properties in the
* output. The `__proto__` field is accepted only when it is an own property.
* Parsing does not guarantee that output keys retain their input order.
*
* **Example** (Defining a basic struct)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const Person = Schema.Struct({
*   name: Schema.String,
*   age: Schema.Number,
*   email: Schema.optionalKey(Schema.String)
* })
*
* // { readonly name: string; readonly age: number; readonly email?: string }
* type Person = typeof Person.Type
*
* Schema.decodeUnknownSync(Person)({ name: "Alice", age: 30 }) // => { name: "Alice", age: 30 }
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function Struct(fields) {
	return makeStruct(struct(fields, void 0), fields);
}
function decodeTo(to, transformation) {
	return (from) => {
		return make$4(decodeTo$1(from.ast, to.ast, transformation ? makeTransformation(transformation) : passthrough()), {
			from,
			to
		});
	};
}
/**
* Attaches a constructor default value to a schema field.
*
* **Details**
*
* Constructor defaults are applied only during `make*`, not during decoding or
* encoding. Failures are represented directly as `SchemaIssue.Issue` values.
*
* **Example** (Defining an optional field with a static default)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* const MySchema = Schema.Struct({
*   name: Schema.String.pipe(
*     Schema.optionalKey,
*     Schema.withConstructorDefault(Effect.succeed("anonymous"))
*   )
* })
*
* MySchema.make({}).name // => "anonymous"
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function withConstructorDefault(defaultValue) {
	return (schema) => make$4(withConstructorDefault$1(schema.ast, defaultValue), { schema });
}
/**
* Combines a {@link Literal} schema with {@link withConstructorDefault}, making it ideal
* for discriminator fields in tagged unions. When constructing via `make`, the
* `_tag` field can be omitted and will be filled automatically.
*
* **Example** (Defining a discriminated union tag)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const A = Schema.Struct({ _tag: Schema.tag("A"), value: Schema.Number })
*
* // _tag is optional in make, auto-filled to "A"
* const a = A.make({ value: 42 })
* a // => { _tag: "A", value: 42 }
* ```
*
* @see {@link tagDefaultOmit} to also omit the tag during encoding
* @see {@link TaggedStruct} for a shorthand that adds `_tag` automatically
* @stability stable
* @category constructors
* @since 3.10.0
*/
function tag(literal) {
	return Literal(literal).pipe(withConstructorDefault(succeed$1(literal)));
}
/**
* Creates a struct schema with an automatically populated `_tag` field.
*
* **When to use**
*
* Use to define a tagged union case from a literal tag and a set of fields.
*
* **Details**
*
* When using the `make` method, the `_tag` field is optional and will be
* added automatically. However, when decoding or encoding, the `_tag` field
* must be present in the input.
*
* **Example** (Defining a tagged struct shorthand)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* // Defines a struct with a fixed `_tag` field
* const tagged = Schema.TaggedStruct("A", {
*   a: Schema.String
* })
*
* // This is the same as writing:
* const equivalent = Schema.Struct({
*   _tag: Schema.tag("A"),
*   a: Schema.String
* })
* void tagged
* void equivalent
* ```
*
* **Example** (Accessing the literal value of the tag)
*
* ```ts import.meta.vitest
* import { Schema } from "effect"
*
* const tagged = Schema.TaggedStruct("A", {
*   a: Schema.String
* })
*
* tagged.fields._tag.schema.literal // => "A"
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
function TaggedStruct(value, fields) {
	return Struct({
		_tag: tag(value),
		...fields
	});
}
const getErrorOptionsKey = (options) => (options?.includeStack === true ? 1 : 0) | (options?.excludeCause === true ? 2 : 0);
const getErrorOptions = (key) => {
	switch (key) {
		case 0: return;
		case 1: return { includeStack: true };
		case 2: return { excludeCause: true };
		case 3: return {
			includeStack: true,
			excludeCause: true
		};
	}
};
const defectSchemaCache = [];
/**
* Schema for unexpected defect values represented as `unknown` with a JSON
* encoded form.
*
* **When to use**
*
* Use when you need a schema for `Cause` defects or other unexpected failures
* whose runtime value may be any value.
*
* **Details**
*
* The encoded side is {@link Json}. During decoding, JSON objects with a string
* `message` property are decoded into JavaScript `Error` values, preserving a
* non-default `name` and any string `stack`. Other JSON values decode
* unchanged.
*
* During encoding, JavaScript `Error` values encode to JSON objects with
* `name`, `message`, and optional `cause` properties. Pass
* `{ includeStack: true }` to include string stack traces in encoded `Error`
* defects, or `{ excludeCause: true }` to omit causes. Other values are
* serialized through Effect's JSON formatter and then parsed back into JSON
* when possible.
*
* **Gotchas**
*
* This schema is for carrying defects across JSON boundaries, not for
* preserving every JavaScript value exactly. Some values cannot round-trip
* unchanged:
*
* - A non-`Error` object such as `{ message: "boom" }` encodes as an
*   error-shaped JSON object and decodes back as an `Error`.
* - JSON serialization normalizes unsupported values. For example,
*   `undefined` array elements encode as `null`, unsupported object properties
*   are omitted, and circular references are dropped.
* - Values that cannot be represented as JSON fall back to Effect's formatted
*   string representation.
*
* @see {@link ErrorInstance} for a schema that only accepts JavaScript `Error` values.
* @stability stable
* @category schemas
* @since 4.0.0
*/
function Defect(options) {
	const key = getErrorOptionsKey(options);
	const cached = defectSchemaCache[key];
	if (cached !== void 0) return cached;
	const schema = Json.pipe(decodeTo(Unknown, defectFromJson(getErrorOptions(key))));
	defectSchemaCache[key] = schema;
	return schema;
}
globalThis.RegExp;
globalThis.URL;
globalThis.File;
globalThis.URLSearchParams;
globalThis.Uint8Array;
const immerable = /*#__PURE__*/ globalThis.Symbol.for("immer-draftable");
const payloadToken = {};
function makeClass(Inherited, identifier, struct$1, annotations, proto) {
	const getClassSchema = getClassSchemaFactory(struct$1, identifier, annotations);
	const ClassTypeId = getClassTypeId(identifier);
	const out = class extends Inherited {
		constructor(...[input, options]) {
			const payload = options?.["~payload"];
			const value = payload?.token === payloadToken ? payload.value : struct$1.make(input ?? {}, options);
			super(value, {
				...options,
				disableChecks: true,
				"~payload": {
					token: payloadToken,
					value
				}
			});
		}
		static [TypeId$5] = TypeId$5;
		get [ClassTypeId]() {
			return ClassTypeId;
		}
		static [immerable] = true;
		static identifier = identifier;
		static fields = struct$1.fields;
		static get ast() {
			return getClassSchema(this).ast;
		}
		static pipe() {
			return pipeArguments(this, arguments);
		}
		static rebuild(ast) {
			return getClassSchema(this).rebuild(ast);
		}
		static make(input, options) {
			return makeClassSync(toType(getClassSchema(this).ast), input ?? {}, options, (fields) => makeClassTrusted(this, fields));
		}
		static makeOption(input, options) {
			return makeOption(getClassSchema(this))(input ?? {}, options);
		}
		static makeEffect(input, options) {
			return getClassSchema(this).makeEffect(input ?? {}, options);
		}
		static annotate(annotations) {
			return this.rebuild(annotate(this.ast, annotations));
		}
		static annotateKey(annotations) {
			return this.rebuild(annotateKey(this.ast, annotations));
		}
		static check(...checks) {
			return this.rebuild(appendChecks(this.ast, checks));
		}
		static extend(identifier) {
			return (schema, annotations) => {
				const extension = isStruct(schema) ? schema : Struct(schema);
				const fields = {
					...struct$1.fields,
					...extension.fields
				};
				const ast = struct(fields, struct$1.ast.checks, { identifier });
				return makeClass(this, identifier, makeStruct(appendChecks(ast, extension.ast.checks), fields), annotations, proto);
			};
		}
		static mapFields(f, options) {
			return struct$1.mapFields(f, options);
		}
	};
	if (proto !== void 0) Object.assign(out.prototype, proto(identifier));
	return out;
}
/**
* Constructs a class instance from fields that were already validated against
* the class struct, without running the constructor's schema pass.
*
* @internal
*/
function makeClassTrusted(self, fields) {
	return new self(fields, { "~payload": {
		token: payloadToken,
		value: fields
	} });
}
function getClassTransformation(self) {
	return new Transformation(transform$1((input) => makeClassTrusted(self, input)), passthrough$1());
}
function getClassTypeId(identifier) {
	return `~effect/Schema/Class/${identifier}`;
}
function getClassSchemaFactory(from, identifier, annotations) {
	let memo;
	return (self) => {
		if (memo !== void 0) return memo;
		const ClassTypeId = getClassTypeId(identifier);
		const isClassValue = (input) => input instanceof self || hasProperty(input, ClassTypeId);
		const transformation = getClassTransformation(self);
		return memo = decodeTo(make$4(new Declaration([from.ast], () => (input, ast, options) => {
			return isClassValue(input) ? succeed$1(input) : fail$1(new InvalidType(ast, input, options));
		}, {
			identifier,
			[CONSTRUCTOR_ANNOTATION_KEY]: ([from]) => ({
				isConstructed: isClassValue,
				link: new Link(from, transformation)
			}),
			toCodec: ([from]) => new Link(from.ast, transformation),
			toEquivalence: ([from]) => from,
			toFormatter: ([from]) => (t) => `${self.identifier}(${from(t)})`,
			[SENTINELS_ANNOTATION_KEY]: collectSentinels(from.ast),
			...annotations
		})), transformation)(from);
	};
}
function isStruct(schema) {
	return isSchema(schema);
}
/**
* Creates a schema-backed error class that can be used as a typed,
* yieldable error in Effect programs. Combines {@link Class} validation with
* the `YieldableError` interface so instances can be yielded directly inside
* `Effect.gen`.
*
* **Example** (Schema-backed error)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* class NotFound extends Schema.Error<NotFound>("NotFound")({
*   id: Schema.Number
* }) {}
*
* const program = Effect.gen(function*() {
*   yield* new NotFound({ id: 1 })
* })
* const error = await Effect.runPromise(Effect.flip(program))
* error.id // => 1
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const Error$1 = (identifier) => (schema, annotations) => {
	const struct = isStruct(schema) ? schema : Struct(schema);
	return makeClass(Error$3, identifier, struct, annotations, (identifier) => ({ name: identifier }));
};
/**
* Defines a schema-backed yieldable error class with an automatically populated
* `_tag` field.
*
* **When to use**
*
* Use to define typed errors that are schema validated, yielded in `Effect.gen`,
* and matched as tagged union members.
*
* **Example** (Defining a tagged error class)
*
* ```ts import.meta.vitest
* import { Effect, Schema } from "effect"
*
* class NotFound extends Schema.TaggedError<NotFound>()("NotFound", {
*   id: Schema.Number
* }) {}
*
* const program = Effect.gen(function*() {
*   yield* new NotFound({ id: 42 })
* })
* const error = await Effect.runPromise(Effect.flip(program))
* error._tag // => "NotFound"
* error.id // => 42
* ```
*
* @stability stable
* @category constructors
* @since 3.10.0
*/
const TaggedError = (identifier) => {
	return (tagValue, schema, annotations) => {
		const struct = isStruct(schema) ? schema.mapFields((fields) => ({
			_tag: tag(tagValue),
			...fields
		}), { unsafePreserveChecks: true }) : TaggedStruct(tagValue, schema);
		return Error$1(identifier ?? tagValue)(struct, annotations);
	};
};
/**
* Schema that accepts and validates any immutable JSON-compatible value.
*
* **Example** (Validating a JSON value)
*
* ```ts import.meta.vitest
* import { Option, Schema } from "effect"
*
* Schema.decodeUnknownOption(Schema.Json)({ key: [1, true, null] }) // => Option.some({ key: [1, true, null] })
* ```
*
* @stability stable
* @category schemas
* @since 4.0.0
*/
const Json = /*#__PURE__*/ make$4(/*#__PURE__*/ annotate(Json$1, { toCode: () => ({
	runtime: "Schema.Json",
	Type: "Schema.Json"
}) }));
// node_modules/effect/dist/MutableList.js
/**
* Mutable lists for collecting ordered values and draining them from the front.
* A `MutableList<A>` can append values to the end, prepend values to the
* beginning, take one or more values from the front, inspect its contents as an
* array, filter values, remove values, and clear itself. All operations update
* the same list object in place and keep its `length` field current. Taking
* from an empty list returns the `Empty` symbol.
*
* @stability stable
* @since 4.0.0
*/
/**
* Defines the unique symbol used to represent an empty result when taking elements from a MutableList.
* This symbol is returned by `take` when the list is empty, allowing for safe type checking.
*
* **When to use**
*
* Use to detect that `take` returned no element before handling the result as a
* list item.
*
* **Example** (Checking for empty results)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
*
* MutableList.take(list) === MutableList.Empty // => true
* ```
*
* @stability stable
* @category symbols
* @since 4.0.0
*/
const Empty = /*#__PURE__*/ Symbol.for("effect/MutableList/Empty");
/**
* Creates an empty MutableList.
*
* **Example** (Creating an empty mutable list)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
*
* list.length // => 0
* MutableList.append(list, "first")
* MutableList.take(list) // => "first"
* list.length // => 0
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$3 = () => ({
	head: void 0,
	tail: void 0,
	length: 0
});
const compactHead = (self) => {
	const head = self.head;
	if (head.offset >= 1024 && head === self.tail && head.mutable && (head.array.length - head.offset) * 8 <= head.offset) self.head = self.tail = {
		array: head.array.slice(head.offset),
		mutable: true,
		offset: 0,
		next: void 0
	};
};
const emptyBucket = () => ({
	array: [],
	mutable: true,
	offset: 0,
	next: void 0
});
/**
* Appends an element to the end of the MutableList.
* This operation is optimized for high-frequency usage.
*
* **Example** (Appending elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.append(list, 1)
* MutableList.append(list, 2)
* MutableList.append(list, 3)
*
* MutableList.toArray(list) // => [1, 2, 3]
* list.length // => 3
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const append = (self, message) => {
	if (!self.tail) self.head = self.tail = emptyBucket();
	else if (!self.tail.mutable) {
		self.tail.next = emptyBucket();
		self.tail = self.tail.next;
	}
	self.tail.array.push(message);
	self.length++;
};
/**
* Prepends an element to the beginning of the MutableList.
* This operation is optimized for high-frequency usage.
*
* **Example** (Prepending elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.append(list, "last")
* MutableList.prepend(list, "third")
* MutableList.prepend(list, "second")
* MutableList.prepend(list, "first")
*
* MutableList.toArray(list) // => ["first", "second", "third", "last"]
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const prepend = (self, message) => {
	self.head = {
		array: [message],
		mutable: true,
		offset: 0,
		next: self.head
	};
	if (!self.tail) self.tail = self.head;
	self.length++;
};
/**
* Removes all elements from the MutableList, resetting it to an empty state.
* This operation is highly optimized and releases all internal memory.
*
* **Example** (Clearing a mutable list)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5])
*
* MutableList.clear(list)
*
* MutableList.toArray(list) // => []
* list.length // => 0
* MutableList.take(list) === MutableList.Empty // => true
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const clear = (self) => {
	self.head = self.tail = void 0;
	self.length = 0;
};
/**
* Takes up to N elements from the beginning of the MutableList and returns them as an array.
* The taken elements are removed from the list. This operation is optimized for performance
* and includes zero-copy optimizations when possible.
*
* **Details**
*
* Finite fractional values of `n` are rounded down. `NaN` and non-positive
* values leave the list unchanged and return an empty array.
*
* **Example** (Taking batches)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
*
* MutableList.takeN(list, 3) // => [1, 2, 3]
* MutableList.toArray(list) // => [4, 5, 6, 7, 8, 9, 10]
* list.length // => 7
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const takeN = (self, n) => {
	n = normalize$1(n);
	if (n <= 0 || !self.head) return [];
	n = Math.min(n, self.length);
	if (n === self.length && self.head?.offset === 0 && !self.head.next) {
		const array = self.head.array;
		clear(self);
		return array;
	}
	const array = new Array(n);
	let index = 0;
	let chunk = self.head;
	while (chunk) {
		while (chunk.offset < chunk.array.length) {
			array[index++] = chunk.array[chunk.offset];
			if (chunk.mutable) chunk.array[chunk.offset] = void 0;
			chunk.offset++;
			if (index === n) {
				self.head = chunk.offset === chunk.array.length && chunk.next ? chunk.next : chunk;
				self.length -= n;
				if (self.length === 0) clear(self);
				else compactHead(self);
				return array;
			}
		}
		chunk = chunk.next;
	}
	clear(self);
	return array;
};
/**
* Takes all elements from the MutableList and returns them as an array.
* The list becomes empty after this operation. This is equivalent to takeN(list, list.length).
*
* **Example** (Draining all elements)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["apple", "banana", "cherry"])
*
* MutableList.takeAll(list) // => ["apple", "banana", "cherry"]
* list.length // => 0
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const takeAll$2 = (self) => takeN(self, self.length);
/**
* Takes a single element from the beginning of the MutableList.
* Returns the element if available, or the Empty symbol if the list is empty.
* The taken element is removed from the list.
*
* **Example** (Taking one element)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["first", "second", "third"])
*
* MutableList.take(list) // => "first"
* MutableList.toArray(list) // => ["second", "third"]
* list.length // => 2
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const take = (self) => {
	if (!self.head) return Empty;
	const message = self.head.array[self.head.offset];
	if (self.head.mutable) self.head.array[self.head.offset] = void 0;
	self.head.offset++;
	self.length--;
	if (self.head.offset === self.head.array.length) {
		if (self.head.next) self.head = self.head.next;
		else clear(self);
	} else compactHead(self);
	return message;
};
/**
* Filters the MutableList in place, keeping only elements that satisfy the predicate.
* This operation modifies the list and rebuilds its internal structure for efficiency.
* The predicate receives each element's current index.
*
* **Example** (Filtering in place)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<number>()
* MutableList.appendAll(list, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
*
* MutableList.filter(list, (n) => n % 2 === 0)
*
* MutableList.toArray(list) // => [2, 4, 6, 8, 10]
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const filter = (self, f) => {
	const array = [];
	let chunk = self.head;
	let index = 0;
	while (chunk) {
		for (let i = chunk.offset; i < chunk.array.length; i++) if (f(chunk.array[i], index++)) array.push(chunk.array[i]);
		chunk = chunk.next;
	}
	if (array.length === 0) {
		clear(self);
		return;
	}
	self.head = self.tail = {
		array,
		mutable: true,
		offset: 0,
		next: void 0
	};
	self.length = array.length;
};
/**
* Removes all occurrences of a value from the `MutableList` using JavaScript
* strict equality semantics.
*
* **When to use**
*
* Use when in-place removal should use JavaScript identity/strict equality
* rather than Effect structural equality.
*
* **Details**
*
* The list is modified in place.
*
* **Gotchas**
*
* Values are compared with `!==`, so this does not use Effect structural
* equality.
*
* **Example** (Removing matching values)
*
* ```ts import.meta.vitest
* import { MutableList } from "effect"
*
* const list = MutableList.make<string>()
* MutableList.appendAll(list, ["apple", "banana", "apple", "cherry", "apple"])
*
* MutableList.remove(list, "apple")
*
* MutableList.toArray(list) // => ["banana", "cherry"]
* ```
*
* @stability stable
* @category mutations
* @since 4.0.0
*/
const remove = (self, value) => filter(self, (v) => v !== value);
// node_modules/effect/dist/Queue.js
const TypeId$4 = "~effect/Queue";
const EnqueueTypeId = "~effect/Queue/Enqueue";
const DequeueTypeId = "~effect/Queue/Dequeue";
const variance = {
	_A: identity,
	_E: identity
};
const QueueProto = {
	[TypeId$4]: variance,
	[EnqueueTypeId]: variance,
	[DequeueTypeId]: variance,
	...PipeInspectableProto,
	toJSON() {
		return {
			_id: "effect/Queue",
			state: this.state._tag,
			size: sizeUnsafe(this)
		};
	}
};
/**
* Creates a `Queue` with optional capacity and overflow strategy.
*
* **Details**
*
* By default the queue is unbounded and uses the `"suspend"` strategy. Provide
* `capacity` for a bounded queue and choose `"suspend"`, `"dropping"`, or
* `"sliding"` to control what happens when the queue is full. The returned
* queue can be offered to, taken from, failed, ended, interrupted, or shut down.
*
* **Example** (Creating queues)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.make<number, string | Cause.Done>()
*
*   // add messages to the queue
*   yield* Queue.offer(queue, 1)
*   yield* Queue.offer(queue, 2)
*   yield* Queue.offerAll(queue, [3, 4, 5])
*
*   // take messages from the queue
*   const messages = yield* Queue.takeAll(queue)
*
*   // signal that the queue is done
*   yield* Queue.end(queue)
*   const done = yield* Effect.flip(Queue.take(queue))
*
*   // signal that another queue has failed
*   const failedQueue = yield* Queue.make<number, string>()
*   const failed = yield* Queue.fail(failedQueue, "boom")
*   return { messages, done, failed }
* })
*
* await Effect.runPromise(program) // => { messages: [1, 2, 3, 4, 5], done: Cause.Done(), failed: true }
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const make$2 = (options) => withFiber$1((fiber) => {
	const self = Object.create(QueueProto);
	self.dispatcher = fiber.currentDispatcher;
	self.capacity = options?.capacity ?? Number.POSITIVE_INFINITY;
	self.strategy = options?.strategy ?? "suspend";
	self.messages = make$3();
	self.scheduleRunning = false;
	self.state = {
		_tag: "Open",
		takers: /* @__PURE__ */ new Set(),
		offers: /* @__PURE__ */ new Set(),
		awaiters: /* @__PURE__ */ new Set()
	};
	return succeed$3(self);
});
/**
* Creates an unbounded queue that can grow to any size without blocking producers.
*
* **When to use**
*
* Use when you need producers to add messages without backpressure and accept
* unbounded memory growth.
*
* **Example** (Creating unbounded queues)
*
* ```ts import.meta.vitest
* import { Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.unbounded<string>()
*
*   // Producers can always add messages without blocking
*   yield* Queue.offer(queue, "message1")
*   yield* Queue.offer(queue, "message2")
*   yield* Queue.offerAll(queue, ["message3", "message4", "message5"])
*
*   // Check current size
*   const size = yield* Queue.size(queue)
*
*   // Take all messages
*   const messages = yield* Queue.takeAll(queue)
*   return { size, messages }
* })
*
* await Effect.runPromise(program) // => { size: 5, messages: ["message1", "message2", "message3", "message4", "message5"] }
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const unbounded$1 = () => make$2();
/**
* Adds a message to the queue. Returns `false` if the queue is done.
*
* **Details**
*
* For bounded queues, this operation may suspend if the queue is at capacity,
* depending on the backpressure strategy. For dropping/sliding queues, it may
* return false or succeed immediately by dropping/sliding existing messages.
*
* **Example** (Offering a value)
*
* ```ts import.meta.vitest
* import { Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number>(3)
*
*   // Successfully add messages to queue
*   const success1 = yield* Queue.offer(queue, 1)
*   const success2 = yield* Queue.offer(queue, 2)
*
*   // Queue state
*   const size = yield* Queue.size(queue)
*   return { offered: [success1, success2], size }
* })
*
* await Effect.runPromise(program) // => { offered: [true, true], size: 2 }
* ```
*
* @stability stable
* @category offering
* @since 2.0.0
*/
const offer = /*#__PURE__*/ dual(2, (self, message) => suspend$1(() => offerUnsafe(self, message) ? exitTrue : self.state._tag === "Open" && self.strategy === "suspend" ? offerOrWait(self, message) : exitFalse));
/**
* Adds a message to the queue synchronously. Returns `false` without adding the
* message if the queue is closing or done, or if it is full with a non-sliding
* strategy (`"suspend"` or `"dropping"`). A queue with non-positive capacity can
* still accept a message when a taker is waiting.
*
* **When to use**
*
* Use when you are already in synchronous queue internals or a performance
* boundary where wrapping the mutation in `Effect` is intentionally avoided.
*
* **Gotchas**
*
* This is an unsafe operation that directly modifies the queue without Effect wrapping.
* Use this only when you're certain about the synchronous nature of the operation.
*
* **Example** (Offering a value synchronously)
*
* ```ts import.meta.vitest
* import { Effect, Queue } from "effect"
*
* // Create a queue effect and extract the queue for unsafe operations
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number>(3)
*
*   // Add messages synchronously using unsafe API
*   const success1 = Queue.offerUnsafe(queue, 1)
*   const success2 = Queue.offerUnsafe(queue, 2)
*
*   // Check current size
*   const size = Queue.sizeUnsafe(queue)
*   return { offered: [success1, success2], size }
* })
*
* await Effect.runPromise(program) // => { offered: [true, true], size: 2 }
* ```
*
* @stability stable
* @category offering
* @since 4.0.0
*/
const offerUnsafe = (self, message) => {
	if (self.state._tag !== "Open") return false;
	else if (self.messages.length >= self.capacity) {
		if (self.strategy === "sliding") {
			take(self.messages);
			append(self.messages, message);
			return true;
		} else if (self.capacity <= 0 && self.state.takers.size > 0) {
			append(self.messages, message);
			releaseTakers(self);
			return true;
		}
		return false;
	}
	append(self.messages, message);
	scheduleReleaseTaker(self);
	return true;
};
/**
* Takes all currently available messages, waiting until at least one message
* is available when the queue is empty.
*
* **When to use**
*
* Use when consumers should process the next non-empty batch of buffered
* messages instead of repeatedly taking one message at a time.
*
* **Details**
*
* Returns a non-empty array. If the queue completes or fails before a message
* can be taken, the effect fails with the queue's terminal error.
*
* **Example** (Taking all available values)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number, Cause.Done>(5)
*
*   // Add several messages
*   yield* Queue.offerAll(queue, [1, 2, 3, 4, 5])
*
*   // Take all available messages
*   const messages1 = yield* Queue.takeAll(queue)
*   return messages1
* })
*
* await Effect.runPromise(program) // => [1, 2, 3, 4, 5]
* ```
*
* @stability stable
* @category taking
* @since 2.0.0
*/
const takeAll$1 = (self) => takeBetween(self, 1, Number.POSITIVE_INFINITY);
/**
* Takes between `min` and `max` messages from the queue.
*
* **Details**
*
* The operation waits when fewer than the required minimum messages are
* available. It returns at most `max` messages. Finite fractional bounds are
* rounded down, while `NaN` and non-positive bounds are treated as `0`. If the
* queue is closing, drains the currently available messages even when fewer
* than `min` are available. Once the queue is done, the effect fails with the
* queue's terminal error.
*
* **Example** (Taking a bounded batch of values)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number>(10)
*
*   // Add several messages
*   yield* Queue.offerAll(queue, [1, 2, 3, 4, 5, 6, 7, 8])
*
*   // Take between 2 and 5 messages
*   const batch1 = yield* Queue.takeBetween(queue, 2, 5)
*
*   // Take between 1 and 10 messages (but only 3 remain)
*   const batch2 = yield* Queue.takeBetween(queue, 1, 10)
*
*   // No more messages available, will wait or return done
*   // const batch3 = yield* Queue.takeBetween(queue, 1, 3)
*   return [batch1, batch2]
* })
*
* await Effect.runPromise(program) // => [[1, 2, 3, 4, 5], [6, 7, 8]]
* ```
*
* @stability stable
* @category taking
* @since 2.0.0
*/
const takeBetween = /*#__PURE__*/ dual(3, (self, min, max) => {
	min = normalize$1(min);
	max = normalize$1(max);
	return suspendTake(() => takeBetweenUnsafe(self, min, max) ?? andThen$1(awaitTake(self, () => canTake(self, min)), takeBetween(self, min, max)));
});
/**
* Returns the current number of buffered messages in the queue synchronously.
*
* **When to use**
*
* Use when you need an immediate `Queue` size snapshot for diagnostics or
* internals and do not need the read wrapped in `Effect`.
*
* **Details**
*
* After `endUnsafe`, a queue remains `Closing` while buffered messages are
* drained, and its size continues to include those messages. A `Done` queue
* reports a size of `0`. This unsafe operation reads the queue state directly
* without Effect wrapping.
*
* **Example** (Checking queue size synchronously)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number, Cause.Done>(10)
*
*   // Check size of empty queue
*   const size1 = Queue.sizeUnsafe(queue)
*
*   // Add some messages
*   Queue.offerUnsafe(queue, 1)
*   Queue.offerUnsafe(queue, 2)
*   Queue.offerUnsafe(queue, 3)
*
*   // Check size after adding messages
*   const size2 = Queue.sizeUnsafe(queue)
*
*   // End the queue
*   Queue.endUnsafe(queue)
*
*   // Ending retains the buffered size while the queue is Closing
*   const size3 = Queue.sizeUnsafe(queue)
*   return [size1, size2, size3]
* })
*
* await Effect.runPromise(program) // => [0, 3, 3]
* ```
*
* @stability stable
* @category sizes
* @since 4.0.0
*/
const sizeUnsafe = (self) => self.state._tag === "Done" ? 0 : self.messages.length;
const exitFalse = /*#__PURE__*/ exitSucceed(false);
const exitTrue = /*#__PURE__*/ exitSucceed(true);
const releaseTakers = (self) => {
	if (self.state._tag === "Done" || self.state.takers.size === 0) return;
	for (const taker of self.state.takers) {
		if (!taker.ready()) continue;
		self.state.takers.delete(taker);
		taker.resume(exitVoid);
		if (self.messages.length === 0) break;
	}
};
const scheduleReleaseTaker = (self) => {
	if (self.scheduleRunning || self.state._tag === "Done" || self.state.takers.size === 0) return;
	self.scheduleRunning = true;
	self.dispatcher.scheduleTask(() => {
		self.scheduleRunning = false;
		releaseTakers(self);
	}, 0);
};
const takeBetweenUnsafe = (self, min, max) => {
	if (self.state._tag === "Done") return self.state.exit;
	else if (max <= 0 || min <= 0) return exitSucceed([]);
	else if (!canTake(self, min)) return;
	const messages = self.messages.length > 0 ? takeN(self.messages, max) : [takeOfferUnsafe(self.state.offers)];
	releaseCapacity(self);
	return exitSucceed(messages);
};
const suspendTake = /*#__PURE__*/ makePrimitive({
	op: "QueueTake",
	[evaluate](fiber) {
		const effect = this[args]();
		return isExit(effect) ? effect[evaluate](fiber) : effect;
	}
});
const canTake = (self, min) => self.messages.length >= (self.state._tag === "Closing" ? 1 : Math.min(min, self.capacity || 1)) || self.capacity <= 0 && self.state._tag !== "Done" && self.state.offers.size > 0;
const awaitTake = (self, ready) => callback$1((resume) => {
	if (self.state._tag === "Done") return resume(self.state.exit);
	if (ready()) return resume(exitVoid);
	const taker = {
		ready,
		resume
	};
	self.state.takers.add(taker);
	return sync$1(() => {
		if (self.state._tag !== "Done") self.state.takers.delete(taker);
	});
});
const offerOrWait = (self, message) => callback$1((resume) => offerUnsafe(self, message) ? resume(exitTrue) : waitToOffer(self, {
	_tag: "Single",
	message,
	resume
}));
const waitToOffer = (self, entry) => {
	if (self.state._tag !== "Open") return resumeUnoffered(entry);
	const offers = self.state.offers;
	offers.add(entry);
	return sync$1(() => {
		if (self.state._tag === "Done") return;
		offers.delete(entry);
		if (self.state._tag === "Closing" && offers.size === 0 && self.messages.length === 0) finalize(self, self.state.exit);
	});
};
const resumeUnoffered = (entry) => entry._tag === "Single" ? entry.resume(exitFalse) : entry.resume(exitSucceed(entry.remaining.slice(entry.offset)));
const takeOfferUnsafe = (offers) => {
	const entry = offers.values().next().value;
	if (entry._tag === "Single") {
		offers.delete(entry);
		entry.resume(exitTrue);
		return entry.message;
	}
	const message = entry.remaining[entry.offset++];
	if (entry.offset === entry.remaining.length) {
		offers.delete(entry);
		entry.resume(exitSucceed([]));
	}
	return message;
};
const releaseCapacity = (self) => {
	if (self.state._tag === "Done") return isDoneCause(self.state.exit.cause);
	else if (self.state.offers.size === 0) {
		if (self.state._tag === "Closing" && self.messages.length === 0) {
			finalize(self, self.state.exit);
			return isDoneCause(self.state.exit.cause);
		}
		return false;
	}
	for (const entry of self.state.offers) {
		let n = self.capacity - self.messages.length;
		if (n <= 0) break;
		else if (entry._tag === "Single") {
			append(self.messages, entry.message);
			self.state.offers.delete(entry);
			entry.resume(exitTrue);
		} else {
			for (; entry.offset < entry.remaining.length; entry.offset++) {
				if (n === 0) return false;
				append(self.messages, entry.remaining[entry.offset]);
				n--;
			}
			self.state.offers.delete(entry);
			entry.resume(exitSucceed([]));
		}
		const state = self.state;
		if (state._tag === "Done") return isDoneCause(state.exit.cause);
	}
	return false;
};
const finalize = (self, exit) => {
	if (self.state._tag === "Done") return;
	const openState = self.state;
	self.state = {
		_tag: "Done",
		exit
	};
	for (const taker of openState.takers) taker.resume(exit);
	openState.takers.clear();
	for (const awaiter of openState.awaiters) awaiter(exit);
	openState.awaiters.clear();
};
// node_modules/effect/dist/Latch.js
/**
* Creates a `Latch` synchronously, outside of `Effect`.
*
* **When to use**
*
* Use when you need to allocate a `Latch` synchronously outside an Effect
* workflow.
*
* **Details**
*
* The latch starts closed by default; pass `true` to create it open.
*
* **Example** (Creating a latch unsafely)
*
* ```ts import.meta.vitest
* import { Effect, Fiber, Latch } from "effect"
*
* const latch = Latch.makeUnsafe(false)
* const waiter = latch.await.pipe(Effect.as("opened"))
*
* const program = Effect.gen(function*() {
*   const fiber = yield* Effect.forkChild(waiter)
*   yield* latch.open
*   return yield* Fiber.join(fiber)
* })
*
* await Effect.runPromise(program) // => "opened"
* ```
*
* @see {@link make} for creating a latch inside Effect code
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeUnsafe = makeLatchUnsafe;
// node_modules/effect/dist/MutableRef.js
const TypeId$3 = "~effect/MutableRef";
const MutableRefProto = {
	[TypeId$3]: TypeId$3,
	...PipeInspectableProto,
	toJSON() {
		return {
			_id: "MutableRef",
			current: toJson(this.current)
		};
	}
};
/**
* Creates a new MutableRef with the specified initial value.
*
* **When to use**
*
* Use to create a synchronous `MutableRef` initialized with a value.
*
* **Example** (Creating mutable refs)
*
* ```ts import.meta.vitest
* import { MutableRef } from "effect"
*
* // Create a counter reference
* const counter = MutableRef.make(0)
*
* MutableRef.get(counter) // => 0
*
* // Create a configuration reference
* const config = MutableRef.make({ debug: false, timeout: 5000 })
*
* MutableRef.get(config) // => { debug: false, timeout: 5000 }
*
* // Create a string reference
* const status = MutableRef.make("idle")
* MutableRef.set(status, "running")
*
* MutableRef.get(status) // => "running"
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const make$1 = (value) => {
	const ref = Object.create(MutableRefProto);
	ref.current = value;
	return ref;
};
/**
* Sets the MutableRef to a new value and returns the reference.
*
* **When to use**
*
* Use when you need an in-place `MutableRef` replacement that returns the same
* `MutableRef`.
*
* **Example** (Setting values)
*
* ```ts import.meta.vitest
* import { MutableRef } from "effect"
*
* const ref = MutableRef.make("initial")
*
* // Set a new value
* MutableRef.set(ref, "updated")
*
* MutableRef.get(ref) // => "updated"
*
* // Chain set operations (since it returns the ref)
* const result = MutableRef.set(ref, "final")
*
* result === ref // => true
* MutableRef.get(ref) // => "final"
*
* // Set complex objects
* const config = MutableRef.make({ debug: false, verbose: false })
* MutableRef.set(config, { debug: true, verbose: true })
*
* MutableRef.get(config) // => { debug: true, verbose: true }
*
* // Pipe-able version
* const setValue = MutableRef.set("new value")
* setValue(ref)
*
* MutableRef.get(ref) // => "new value"
*
* // Useful for state management
* const state = MutableRef.make<"idle" | "loading" | "success" | "error">("idle")
* MutableRef.set(state, "loading")
* // ... perform async operation
* MutableRef.set(state, "success")
*
* MutableRef.get(state) // => "success"
* ```
*
* @stability stable
* @category mutations
* @since 2.0.0
*/
const set = /*#__PURE__*/ dual(2, (self, value) => {
	self.current = value;
	return self;
});
// node_modules/effect/dist/PubSub.js
/**
* Broadcasts values from publishers to many subscribers.
*
* Publishers add messages with `publish` or `publishAll`, and each active
* `Subscription` receives its own copy of every accepted message. Unlike a
* queue, subscribers do not compete for messages. This module includes bounded,
* dropping, sliding, and unbounded hubs, optional replay buffers for late
* subscribers, message-taking helpers, capacity and shutdown operations, a
* type guard, and low-level types for custom hub strategies.
*
* @stability stable
* @since 2.0.0
*/
const TypeId$2 = "~effect/PubSub";
const SubscriptionTypeId = "~effect/PubSub/Subscription";
/**
* Creates a PubSub with a custom atomic implementation and strategy.
*
* **Example** (Creating a PubSub with a custom strategy)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.gen(function*() {
*   // Create custom PubSub with specific atomic implementation and strategy
*   const pubsub = yield* PubSub.make<string>({
*     atomicPubSub: () => PubSub.makeAtomicBounded(100),
*     strategy: () => new PubSub.BackPressureStrategy()
*   })
*
*   // Use the created PubSub
*   const published = yield* PubSub.publish(pubsub, "Hello")
*   yield* PubSub.shutdown(pubsub)
*   return published
* })
*
* const actual = await Effect.runPromise(program)
* actual // => true
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const make = (options) => sync(() => makePubSubUnsafe(options.atomicPubSub(), /* @__PURE__ */ new Map(), makeUnsafe$3(), makeUnsafe(false), make$1(false), options.strategy(), make$1(none())));
/**
* Creates an unbounded `PubSub`.
*
* **Example** (Creating an unbounded PubSub)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   // Create unbounded PubSub
*   const pubsub = yield* PubSub.unbounded<string>()
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Can publish unlimited messages
*   for (let i = 0; i < 3; i++) {
*     yield* PubSub.publish(pubsub, `message-${i}`)
*   }
*
*   return yield* PubSub.takeAll(subscription)
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => ["message-0", "message-1", "message-2"]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const unbounded = (options) => make({
	atomicPubSub: () => makeAtomicUnbounded(options),
	strategy: () => new DroppingStrategy()
});
/**
* Creates an unbounded atomic PubSub implementation with optional replay buffer.
*
* **When to use**
*
* Use to create the low-level storage layer for a custom `PubSub` whose active
* subscribers may retain an unbounded number of pending messages.
*
* **Gotchas**
*
* Messages published while subscribers are active can be retained without a
* capacity limit until those subscribers take them or unsubscribe.
*
* @see {@link makeAtomicBounded} for a bounded atomic implementation that enforces capacity
* @see {@link make} for wrapping an atomic implementation with a delivery strategy
* @see {@link unbounded} for the high-level effectful constructor for unbounded `PubSub` values
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const makeAtomicUnbounded = (options) => {
	const replay = options?.replay;
	return new UnboundedPubSub(replay && replay > 0 ? new ReplayBuffer(Math.ceil(replay)) : void 0);
};
/**
* Publishes a message to the `PubSub` as an `Effect`, returning whether the
* message was accepted.
*
* **When to use**
*
* Use when you need to publish from effectful code and let the configured
* PubSub strategy handle surplus messages.
*
* **Details**
*
* The effect succeeds with `false` if the `PubSub` is shut down. If the message
* cannot be accepted immediately, the configured strategy decides how surplus
* messages are handled.
*
* **Example** (Publishing a message)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   // Publish a message
*   const published = yield* PubSub.publish(pubsub, "Hello World")
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   yield* PubSub.publish(pubsub, "Hello")
*   const message = yield* PubSub.take(subscription)
*   return { published, message }
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => { published: true, message: "Hello" }
* ```
*
* @see {@link publishUnsafe} for a synchronous non-blocking attempt that does not run effectful surplus handling
*
* @stability stable
* @category publishing
* @since 2.0.0
*/
const publish = /*#__PURE__*/ dual(2, (self, value) => suspend(() => {
	if (self.shutdownFlag.current || isSome(self.ended.current)) return succeed$1(false);
	if (self.pubsub.publish(value)) {
		self.strategy.completeSubscribersUnsafe(self.pubsub, self.subscribers);
		return succeed$1(true);
	}
	return self.strategy.handleSurplus(self.pubsub, self.subscribers, [value], self.shutdownFlag, self.ended);
}));
/**
* Subscribes to receive messages from the `PubSub`. The resulting subscription can
* be evaluated multiple times within the scope to take a message from the `PubSub`
* each time.
*
* **Example** (Subscribing to messages)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   // Subscribe within a scope for automatic cleanup
*   const first = yield* Effect.scoped(Effect.gen(function*() {
*     const subscription = yield* PubSub.subscribe(pubsub)
*
*     // Publish some messages
*     yield* PubSub.publish(pubsub, "Hello")
*     yield* PubSub.publish(pubsub, "World")
*
*     // Take messages one by one
*     const msg1 = yield* PubSub.take(subscription)
*     const msg2 = yield* PubSub.take(subscription)
*
*     // Subscription is automatically cleaned up when scope exits
*     return [msg1, msg2]
*   }))
*
*   const second = yield* Effect.scoped(Effect.gen(function*() {
*     const sub1 = yield* PubSub.subscribe(pubsub)
*     const sub2 = yield* PubSub.subscribe(pubsub)
*
*     // Multiple subscribers can receive the same messages
*     yield* PubSub.publish(pubsub, "Broadcast")
*
*     return yield* Effect.all([
*       PubSub.take(sub1),
*       PubSub.take(sub2)
*     ])
*   }))
*   return [first, second]
* })
*
* const actual = await Effect.runPromise(program)
* actual // => [["Hello", "World"], ["Broadcast", "Broadcast"]]
* ```
*
* @stability stable
* @category subscriptions
* @since 2.0.0
*/
const subscribe = (self) => uninterruptible(contextWith((services) => {
	const localScope = get(services, Scope);
	const scope = forkUnsafe(self.scope);
	const subscription = makeSubscriptionUnsafe(self.pubsub, self.subscribers, self.strategy, self.ended);
	return addFinalizer$1(scope, unsubscribe(subscription)).pipe(andThen(addFinalizerExit(localScope, (exit) => close(scope, exit))), as(subscription));
}));
const unsubscribe = (self) => uninterruptible(withFiber((state) => {
	set(self.shutdownFlag, true);
	return forEach(takeAll$2(self.pollers), (d) => interruptWith(d, state.id), {
		discard: true,
		concurrency: "unbounded"
	}).pipe(tap(() => sync(() => {
		self.subscribers.delete(self.subscription);
		self.subscription.unsubscribe();
		self.replayWindow.close();
		self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
	})), when(self.shutdownHook.open), asVoid);
}));
/**
* Takes all available messages from the subscription, suspending if no items
* are available.
*
* **Example** (Taking all available messages)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(10)
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Publish multiple messages
*   yield* PubSub.publishAll(pubsub, ["msg1", "msg2", "msg3"])
*
*   // Take all available messages at once
*   return yield* PubSub.takeAll(subscription)
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => ["msg1", "msg2", "msg3"]
* ```
*
* @stability stable
* @category subscriptions
* @since 4.0.0
*/
const takeAll = (self) => suspend(function loop(value) {
	if (self.shutdownFlag.current) return interrupt;
	let as = self.pollers.length === 0 ? self.subscription.pollUpTo(Number.POSITIVE_INFINITY) : [];
	if (value) as = value.concat(as);
	self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
	if (self.replayWindow.remaining > 0) return succeed$1(self.replayWindow.takeAll().concat(as));
	else if (!isArrayNonEmpty(as)) return flatMap(pollForItem(self), (item) => loop([item]));
	return succeed$1(as);
});
const pollForItem = (self) => callback((resume) => {
	if (self.shutdownFlag.current) return resume(interrupt);
	if (isSome(self.ended.current)) {
		const message = self.pollers.length === 0 ? self.subscription.poll() : Empty;
		if (message !== Empty) {
			self.strategy.onPubSubEmptySpaceUnsafe(self.pubsub, self.subscribers);
			return resume(succeed$1(message));
		}
		return resume(succeed$1(self.ended.current.value));
	}
	const deferred = makeUnsafe$4();
	let set = self.subscribers.get(self.subscription);
	if (!set) {
		set = /* @__PURE__ */ new Set();
		self.subscribers.set(self.subscription, set);
	}
	set.add(self.pollers);
	append(self.pollers, deferred);
	self.strategy.completePollersUnsafe(self.pubsub, self.subscribers, self.subscription, self.pollers);
	if (deferred.effect) return resume(deferred.effect);
	deferred.resumes = [resume];
	return sync(() => remove(self.pollers, deferred));
});
const AbsentValue = /*#__PURE__*/ Symbol.for("effect/PubSub/AbsentValue");
const addSubscribers = (subscribers, subscription, pollers) => {
	if (!subscribers.has(subscription)) subscribers.set(subscription, /* @__PURE__ */ new Set());
	subscribers.get(subscription).add(pollers);
};
const removeSubscribers = (subscribers, subscription, pollers) => {
	if (!subscribers.has(subscription)) return;
	const set = subscribers.get(subscription);
	set.delete(pollers);
	if (set.size === 0) subscribers.delete(subscription);
};
const makeSubscriptionUnsafe = (pubsub, subscribers, strategy, ended) => new SubscriptionImpl(pubsub, subscribers, pubsub.subscribe(), make$3(), makeUnsafe(false), make$1(false), strategy, pubsub.replayWindow(), ended);
var UnboundedPubSub = class {
	publisherHead = {
		value: AbsentValue,
		replayIndex: void 0,
		subscribers: 0,
		next: null
	};
	publisherTail = this.publisherHead;
	publisherIndex = 0;
	subscribersIndex = 0;
	capacity = Number.MAX_SAFE_INTEGER;
	replayBuffer;
	constructor(replayBuffer) {
		this.replayBuffer = replayBuffer;
	}
	replayWindow() {
		return this.replayBuffer ? new ReplayWindowImpl(this.replayBuffer) : emptyReplayWindow;
	}
	isEmpty() {
		return this.publisherHead === this.publisherTail;
	}
	isFull() {
		return false;
	}
	size() {
		return this.publisherIndex - this.subscribersIndex;
	}
	publish(value) {
		const replayIndex = this.replayBuffer?.offer(value);
		const subscribers = this.publisherTail.subscribers;
		if (subscribers !== 0) {
			const node = {
				value,
				replayIndex,
				subscribers,
				next: null
			};
			this.publisherTail.next = node;
			this.publisherTail = this.publisherTail.next;
			this.publisherIndex += 1;
		}
		return true;
	}
	publishAll(elements) {
		if (this.publisherTail.subscribers !== 0) for (const a of elements) this.publish(a);
		else if (this.replayBuffer) this.replayBuffer.offerAll(elements);
		return [];
	}
	slide() {
		if (this.publisherHead !== this.publisherTail) {
			const node = this.publisherHead.next;
			const value = node.value;
			this.publisherHead = this.publisherHead.next;
			this.publisherHead.value = AbsentValue;
			this.subscribersIndex += 1;
			this.replayBuffer?.slide(value, node.replayIndex);
		}
	}
	subscribe() {
		this.publisherTail.subscribers += 1;
		return new UnboundedPubSubSubscription(this, this.publisherTail, this.publisherIndex, false);
	}
};
var UnboundedPubSubSubscription = class {
	self;
	subscriberHead;
	subscriberIndex;
	unsubscribed;
	constructor(self, subscriberHead, subscriberIndex, unsubscribed) {
		this.self = self;
		this.subscriberHead = subscriberHead;
		this.subscriberIndex = subscriberIndex;
		this.unsubscribed = unsubscribed;
	}
	isEmpty() {
		if (this.unsubscribed) return true;
		let empty = true;
		let loop = true;
		while (loop) if (this.subscriberHead === this.self.publisherTail) loop = false;
		else if (this.subscriberHead.next.value !== AbsentValue) {
			empty = false;
			loop = false;
		} else {
			this.subscriberHead = this.subscriberHead.next;
			this.subscriberIndex += 1;
		}
		return empty;
	}
	size() {
		if (this.unsubscribed) return 0;
		return this.self.publisherIndex - Math.max(this.subscriberIndex, this.self.subscribersIndex);
	}
	poll() {
		if (this.unsubscribed) return Empty;
		let loop = true;
		let polled = Empty;
		while (loop) if (this.subscriberHead === this.self.publisherTail) loop = false;
		else {
			const elem = this.subscriberHead.next.value;
			if (elem !== AbsentValue) {
				polled = elem;
				this.subscriberHead.subscribers -= 1;
				if (this.subscriberHead.subscribers === 0) {
					this.self.publisherHead = this.self.publisherHead.next;
					this.self.publisherHead.value = AbsentValue;
					this.self.subscribersIndex += 1;
				}
				loop = false;
			}
			this.subscriberHead = this.subscriberHead.next;
			this.subscriberIndex += 1;
		}
		return polled;
	}
	pollUpTo(n) {
		n = normalize$1(n);
		const builder = [];
		let i = 0;
		while (i !== n) {
			const a = this.poll();
			if (a === Empty) i = n;
			else {
				builder.push(a);
				i += 1;
			}
		}
		return builder;
	}
	unsubscribe() {
		if (!this.unsubscribed) {
			this.unsubscribed = true;
			this.self.publisherTail.subscribers -= 1;
			while (this.subscriberHead !== this.self.publisherTail) {
				if (this.subscriberHead.next.value !== AbsentValue) {
					this.subscriberHead.subscribers -= 1;
					if (this.subscriberHead.subscribers === 0) {
						this.self.publisherHead = this.self.publisherHead.next;
						this.self.publisherHead.value = AbsentValue;
						this.self.subscribersIndex += 1;
					}
				}
				this.subscriberHead = this.subscriberHead.next;
			}
		}
	}
};
var SubscriptionImpl = class {
	[SubscriptionTypeId] = { _A: identity };
	pubsub;
	subscribers;
	subscription;
	pollers;
	shutdownHook;
	shutdownFlag;
	strategy;
	replayWindow;
	ended;
	constructor(pubsub, subscribers, subscription, pollers, shutdownHook, shutdownFlag, strategy, replayWindow, ended) {
		this.pubsub = pubsub;
		this.subscribers = subscribers;
		this.subscription = subscription;
		this.pollers = pollers;
		this.shutdownHook = shutdownHook;
		this.shutdownFlag = shutdownFlag;
		this.strategy = strategy;
		this.replayWindow = replayWindow;
		this.ended = ended;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
};
var PubSubImpl = class {
	[TypeId$2] = { _A: identity };
	pubsub;
	subscribers;
	scope;
	shutdownHook;
	shutdownFlag;
	strategy;
	ended;
	constructor(pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended) {
		this.pubsub = pubsub;
		this.subscribers = subscribers;
		this.scope = scope;
		this.shutdownHook = shutdownHook;
		this.shutdownFlag = shutdownFlag;
		this.strategy = strategy;
		this.ended = ended;
	}
	pipe() {
		return pipeArguments(this, arguments);
	}
};
const makePubSubUnsafe = (pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended) => new PubSubImpl(pubsub, subscribers, scope, shutdownHook, shutdownFlag, strategy, ended);
/**
* Represents the dropping strategy for bounded `PubSub` values.
*
* **When to use**
*
* Use to keep publishers fast by dropping new messages when the `PubSub` is at
* capacity.
*
* **Details**
*
* A publish that arrives while the `PubSub` is full is dropped instead of
* waiting for capacity.
*
* **Gotchas**
*
* Subscribers may miss messages published while they are subscribed.
*
* **Example** (Applying a dropping strategy)
*
* ```ts import.meta.vitest
* import { Effect, PubSub } from "effect"
*
* const program = Effect.scoped(Effect.gen(function*() {
*   // Explicitly create a PubSub with a dropping strategy
*   const pubsub = yield* PubSub.make<string>({
*     atomicPubSub: () => PubSub.makeAtomicBounded(2),
*     strategy: () => new PubSub.DroppingStrategy()
*   })
*
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Fill the PubSub
*   const pub1 = yield* PubSub.publish(pubsub, "msg1") // true
*   const pub2 = yield* PubSub.publish(pubsub, "msg2") // true
*   const pub3 = yield* PubSub.publish(pubsub, "msg3") // false (dropped)
*
*   // Subscribers will only see the first two messages
*   const messages = yield* PubSub.takeAll(subscription)
*   return { published: [pub1, pub2, pub3], messages }
* }))
*
* const actual = await Effect.runPromise(program)
* actual // => { published: [true, true, false], messages: ["msg1", "msg2"] }
* ```
*
* @stability stable
* @category models
* @since 4.0.0
*/
var DroppingStrategy = class {
	get shutdown() {
		return void_;
	}
	handleSurplus(_pubsub, _subscribers, _elements, _isShutdown) {
		return succeed$1(false);
	}
	onPubSubEmptySpaceUnsafe(_pubsub, _subscribers) {}
	completePollersUnsafe(pubsub, subscribers, subscription, pollers) {
		return strategyCompletePollersUnsafe(this, pubsub, subscribers, subscription, pollers);
	}
	completeSubscribersUnsafe(pubsub, subscribers) {
		return strategyCompleteSubscribersUnsafe(this, pubsub, subscribers);
	}
};
const strategyCompletePollersUnsafe = (strategy, pubsub, subscribers, subscription, pollers) => {
	let keepPolling = true;
	while (keepPolling && !subscription.isEmpty()) {
		const poller = take(pollers);
		if (poller === Empty) {
			removeSubscribers(subscribers, subscription, pollers);
			if (pollers.length === 0) keepPolling = false;
			else addSubscribers(subscribers, subscription, pollers);
		} else {
			const pollResult = subscription.poll();
			if (pollResult === Empty) prepend(pollers, poller);
			else {
				doneUnsafe(poller, succeed$2(pollResult));
				strategy.onPubSubEmptySpaceUnsafe(pubsub, subscribers);
			}
		}
	}
};
const strategyCompleteSubscribersUnsafe = (strategy, pubsub, subscribers) => {
	for (const [subscription, pollersSet] of subscribers) for (const pollers of pollersSet) strategy.completePollersUnsafe(pubsub, subscribers, subscription, pollers);
};
var ReplayBuffer = class {
	capacity;
	head = {
		value: AbsentValue,
		index: 0,
		next: null
	};
	tail = this.head;
	slideValues = [];
	size = 0;
	index = 0;
	publisherIndex = 0;
	constructor(capacity) {
		this.capacity = capacity;
	}
	slide(value, publisherIndex) {
		this.slideValues[this.index % this.capacity] = {
			value,
			index: publisherIndex
		};
		this.index++;
	}
	offer(a) {
		const index = this.publisherIndex++;
		this.tail.value = a;
		this.tail.index = index;
		this.tail.next = {
			value: AbsentValue,
			index: 0,
			next: null
		};
		this.tail = this.tail.next;
		if (this.size === this.capacity) {
			const dropped = this.head;
			this.head = dropped.next;
			dropped.next = null;
			dropped.value = AbsentValue;
		} else this.size += 1;
		return index;
	}
	offerAll(as) {
		for (const a of as) this.offer(a);
	}
};
var ReplayWindowImpl = class {
	buffer;
	values;
	index = 0;
	remaining;
	slideIndex;
	newestIndex = -1;
	constructor(buffer) {
		this.buffer = buffer;
		this.remaining = buffer.size;
		this.slideIndex = buffer.index;
		this.values = new Array(this.remaining);
		let node = buffer.head;
		for (let i = 0; i < this.remaining; i++) {
			this.values[i] = node.value;
			this.newestIndex = node.index;
			node = node.next;
		}
	}
	close() {
		this.values.length = 0;
		this.remaining = 0;
	}
	sync() {
		const slides = this.buffer.index - this.slideIndex;
		if (slides === 0 || this.remaining === 0) return;
		const count = Math.min(slides, this.buffer.capacity);
		const start = this.buffer.index - count;
		for (let i = 0; i < count; i++) {
			const entry = this.buffer.slideValues[(start + i) % this.buffer.capacity];
			if (entry.index > this.newestIndex) {
				this.index = (this.index + 1) % this.values.length;
				this.values[(this.index + this.remaining - 1) % this.values.length] = entry.value;
				this.newestIndex = entry.index;
			}
		}
		this.slideIndex = this.buffer.index;
	}
	take() {
		if (this.remaining === 0) return;
		this.sync();
		const value = this.values[this.index];
		this.values[this.index] = AbsentValue;
		this.index = (this.index + 1) % this.values.length;
		this.remaining--;
		if (this.remaining === 0) this.close();
		return value;
	}
	takeN(n) {
		n = normalize$1(n);
		const len = Math.min(n, this.remaining);
		const items = new Array(len);
		for (let i = 0; i < len; i++) items[i] = this.take();
		return items;
	}
	takeAll() {
		return this.takeN(this.remaining);
	}
};
const emptyReplayWindow = {
	remaining: 0,
	take: () => void 0,
	takeN: () => [],
	takeAll: () => [],
	close: () => void 0
};
const ChannelProto = {
	["~effect/Channel"]: {
		_Env: identity,
		_InErr: identity,
		_InElem: identity,
		_OutErr: identity,
		_OutElem: identity
	},
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/**
* Creates a `Channel` from a transformation function that operates on upstream pulls.
*
* **Example** (Creating channels from transforms)
*
* ```ts import.meta.vitest
* import { Channel, Effect } from "effect"
*
* const channel = Channel.fromTransform((upstream, scope) =>
*   Effect.succeed(upstream)
* )
* await Effect.runPromise(Channel.runCollect(channel)) // => []
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromTransform = (transform) => {
	const self = Object.create(ChannelProto);
	self.transform = (upstream, scope) => catchCause(transform(upstream, scope), (cause) => succeed$1(failCause(cause)));
	return self;
};
/**
* Creates a `Channel` from an `Effect` that produces a `Pull`.
*
* **Example** (Creating channels from pulls)
*
* ```ts import.meta.vitest
* import { Cause, Channel, Effect } from "effect"
*
* const channel = Channel.fromPull(Effect.sync(() => {
*   let emitted = false
*   return Effect.suspend(() => {
*     if (emitted) return Cause.done()
*     emitted = true
*     return Effect.succeed(42)
*   })
* }))
* await Effect.runPromise(Channel.runCollect(channel)) // => [42]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromPull = (effect) => fromTransform((_, __) => effect);
/**
* Converts a `Channel` back to its underlying transformation function.
*
* **Example** (Extracting channel transforms)
*
* ```ts import.meta.vitest
* import { Channel, Effect } from "effect"
*
* const channel = Channel.succeed(42)
* const transform = Channel.toTransform(channel)
* typeof transform // => "function"
* Effect.runSync(Channel.runCollect(channel)) // => [42]
* ```
*
* @stability stable
* @category destructors
* @since 4.0.0
*/
const toTransform = (channel) => channel.transform;
/**
* Creates a channel from a queue that emits arrays of elements.
*
* **Example** (Creating batched channels from queues)
*
* ```ts import.meta.vitest
* import { Cause, Channel, Effect, Queue } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.bounded<number, Cause.Done>(4)
*   yield* Queue.offerAll(queue, [1, 2, 3, 4])
*   yield* Queue.end(queue)
*   const arrayChannel = Channel.fromQueueArray(queue)
*   return yield* Channel.runCollect(arrayChannel)
* })
* await Effect.runPromise(program) // => [[1, 2, 3, 4]]
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromQueueArray = (queue) => fromPull(succeed$1(takeAll$1(queue)));
/**
* Creates a channel from a PubSub subscription that outputs arrays of values.
*
* **Details**
*
* This constructor creates a channel that reads from a PubSub subscription and outputs
* arrays of values in chunks. It's useful when you want to process multiple values at once
* for better performance.
*
* **Example** (Batching subscription values)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class StreamError extends Data.TaggedError("StreamError")<{
*   readonly message: string
* }> {}
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<number>(16)
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Create a channel that reads arrays of values
*   const channel = Channel.fromSubscriptionArray(subscription)
*
*   // Publish some values
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*   yield* PubSub.publish(pubsub, 4)
*
*   // The channel will output arrays like [1, 2, 3] and [4]
*   return channel
* })
* const result = Effect.scoped(Effect.flatMap(program, Channel.runHead))
* await Effect.runPromise(result) // => Option.some([1, 2, 3, 4])
* ```
*
* **Example** (Processing subscription values in batches)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class BatchProcessingError extends Data.TaggedError("BatchProcessingError")<{
*   readonly reason: string
* }> {}
*
* const batchProcessor = Effect.gen(function*() {
*   const pubsub = yield* PubSub.bounded<string>(32)
*   const subscription = yield* PubSub.subscribe(pubsub)
*
*   // Create a channel that processes items in batches
*   const batchChannel = Channel.fromSubscriptionArray(subscription)
*
*   // Transform to process each batch
*   const processedChannel = Channel.map(batchChannel, (batch) =>
*     batch.map((item) => item.toUpperCase())
*   )
*
*   yield* PubSub.publishAll(pubsub, ["one", "two"])
*   return processedChannel
* })
* const batch = Effect.scoped(Effect.flatMap(batchProcessor, Channel.runHead))
* await Effect.runPromise(batch) // => Option.some(["ONE", "TWO"])
* ```
*
* **Example** (Aggregating subscription metrics)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* const metricsAggregator = Effect.gen(function*() {
*   const metricsPubSub = yield* PubSub.bounded<
*     { timestamp: number; value: number }
*   >(100)
*   const subscription = yield* PubSub.subscribe(metricsPubSub)
*
*   // Create a channel that collects metrics in chunks
*   const metricsChannel = Channel.fromSubscriptionArray(subscription)
*
*   // Transform to calculate aggregate statistics
*   const aggregatedChannel = Channel.map(metricsChannel, (metrics) => {
*     const values = metrics.map((m) => m.value)
*     const sum = values.reduce((a, b) => a + b, 0)
*     const avg = sum / values.length
*     const min = Math.min(...values)
*     const max = Math.max(...values)
*
*     return {
*       count: values.length,
*       sum,
*       average: avg,
*       min,
*       max,
*       firstTimestamp: Math.min(...metrics.map((m) => m.timestamp)),
*       lastTimestamp: Math.max(...metrics.map((m) => m.timestamp))
*     }
*   })
*
*   yield* PubSub.publish(metricsPubSub, { timestamp: 1, value: 10 })
*   return aggregatedChannel
* })
* const metric = Effect.scoped(Effect.flatMap(metricsAggregator, Channel.runHead))
* const result = await Effect.runPromise(metric)
* Option.map(result, ({ count, sum, average, min, max }) => ({ count, sum, average, min, max })) // => Option.some({ count: 1, sum: 10, average: 10, min: 10, max: 10 })
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromSubscriptionArray = (subscription) => fromPull(succeed$1(onInterrupt(takeAll(subscription), () => done())));
/**
* Creates a channel from a PubSub that outputs arrays of values.
*
* **Details**
*
* This constructor creates a channel that reads from a PubSub by automatically
* subscribing to it and collecting values into arrays. The channel outputs
* arrays of values in chunks, making it ideal for batch processing scenarios.
*
* **Example** (Batching PubSub values)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect, Option, PubSub } from "effect"
*
* class BatchError extends Data.TaggedError("BatchError")<{
*   readonly message: string
* }> {}
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.unbounded<number>({ replay: 4 })
*
*   // Create a channel that reads arrays of values
*   const channel = Channel.fromPubSubArray(pubsub)
*
*   // Publish some values
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*   yield* PubSub.publish(pubsub, 4)
*
*   // The channel will output arrays like [1, 2, 3] and [4]
*   return channel
* })
* const result = Effect.scoped(Effect.flatMap(program, Channel.runHead))
* await Effect.runPromise(result) // => Option.some([1, 2, 3, 4])
* ```
*
* **Example** (Processing PubSub orders in batches)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* interface Order {
*   readonly id: string
*   readonly customerId: string
*   readonly items: ReadonlyArray<string>
*   readonly total: number
*   readonly submittedAt: number
* }
*
* const orderBatchProcessor = Effect.gen(function*() {
*   const orderPubSub = yield* PubSub.unbounded<Order>({ replay: 1 })
*
*   // Create a channel that processes orders in batches
*   const orderChannel = Channel.fromPubSubArray(orderPubSub)
*
*   // Transform to process each batch of orders
*   const processedChannel = Channel.map(orderChannel, (orderBatch) => {
*     const totalRevenue = orderBatch.reduce((sum, order) => sum + order.total, 0)
*     const customerCount = new Set(orderBatch.map((order) =>
*       order.customerId
*     )).size
*
*     return {
*       batchSize: orderBatch.length,
*       totalRevenue,
*       uniqueCustomers: customerCount,
*       firstSubmittedAt: Math.min(...orderBatch.map((order) => order.submittedAt)),
*       orders: orderBatch
*     }
*   })
*
*   yield* PubSub.publish(orderPubSub, {
*     id: "1", customerId: "a", items: ["book"], total: 10, submittedAt: 1
*   })
*   return processedChannel
* })
* const order = Effect.scoped(Effect.flatMap(orderBatchProcessor, Channel.runHead))
* const result = await Effect.runPromise(order)
* Option.map(result, (batch) => [batch.batchSize, batch.totalRevenue, batch.uniqueCustomers]) // => Option.some([1, 10, 1])
* ```
*
* **Example** (Processing PubSub logs in batches)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Option, PubSub } from "effect"
*
* interface LogEntry {
*   readonly timestamp: number
*   readonly level: "info" | "warn" | "error"
*   readonly message: string
*   readonly source: string
* }
*
* const logAggregator = Effect.gen(function*() {
*   const logPubSub = yield* PubSub.unbounded<LogEntry>({ replay: 1 })
*
*   // Create a channel that collects logs in batches
*   const logChannel = Channel.fromPubSubArray(logPubSub)
*
*   // Transform to analyze log batches
*   const analysisChannel = Channel.map(logChannel, (logBatch) => {
*     const errorCount = logBatch.filter((log) => log.level === "error").length
*     const warnCount = logBatch.filter((log) => log.level === "warn").length
*     const infoCount = logBatch.filter((log) => log.level === "info").length
*
*     const timeRange = {
*       start: Math.min(...logBatch.map((log) => log.timestamp)),
*       end: Math.max(...logBatch.map((log) => log.timestamp))
*     }
*
*     return {
*       batchId: `${timeRange.start}-${timeRange.end}`,
*       totalEntries: logBatch.length,
*       errorCount,
*       warnCount,
*       infoCount,
*       timeRange,
*       sources: [...new Set(logBatch.map((log) => log.source))]
*     }
*   })
*
*   yield* PubSub.publish(logPubSub, {
*     timestamp: 1,
*     level: "info",
*     message: "ready",
*     source: "app"
*   } satisfies LogEntry)
*   return analysisChannel
* })
* const log = Effect.scoped(Effect.flatMap(logAggregator, Channel.runHead))
* const result = await Effect.runPromise(log)
* Option.map(result, (batch) => [batch.batchId, batch.totalEntries, batch.infoCount]) // => Option.some(["1-1", 1, 1])
* ```
*
* @stability stable
* @category constructors
* @since 4.0.0
*/
const fromPubSubArray = (pubsub) => unwrap(map(subscribe(pubsub), fromSubscriptionArray));
/**
* Constructs a `Channel` from a scoped effect that will result in a
* `Channel` if successful.
*
* **Example** (Unwrapping channel effects)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect } from "effect"
*
* class UnwrapError extends Data.TaggedError("UnwrapError")<{
*   readonly reason: string
* }> {}
*
* // Create an effect that produces a channel
* const channelEffect = Effect.succeed(
*   Channel.fromIterable([1, 2, 3])
* )
*
* // Unwrap the effect to get the channel
* const unwrappedChannel = Channel.unwrap(channelEffect)
*
* Effect.runSync(Channel.runCollect(unwrappedChannel)) // => [1, 2, 3]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const unwrap = (channel) => fromTransform((upstream, scope) => {
	let pull;
	return succeed$1(suspend(() => {
		if (pull) return pull;
		return channel.pipe(provide(scope), flatMap((channel) => toTransform(channel)(upstream, scope)), flatMap((pull_) => pull = pull_));
	}));
});
const runWith = (self, f, onHalt) => suspend(() => {
	const scope = makeUnsafe$3();
	const makePull = toTransform(self)(done(), scope);
	return catchDone(flatMap(makePull, f), onHalt ? onHalt : succeed$1).pipe(onExit((exit) => close(scope, exit)));
});
/**
* Runs a channel and applies an effect to each output element.
*
* **Example** (Running effects for each output)
*
* ```ts import.meta.vitest
* import { Channel, Data, Effect } from "effect"
*
* class ForEachError extends Data.TaggedError("ForEachError")<{
*   readonly element: unknown
* }> {}
*
* // Create a channel with numbers
* const numbersChannel = Channel.fromIterable([1, 2, 3])
*
* // Run forEach to process each element
* const processed: Array<number> = []
* const forEachEffect = Channel.runForEach(
*   numbersChannel,
*   (n) => Effect.sync(() => processed.push(n))
* )
*
* await Effect.runPromise(forEachEffect)
* processed // => [1, 2, 3]
* ```
*
* @stability stable
* @category running
* @since 4.0.0
*/
const runForEach$1 = /*#__PURE__*/ dual(2, (self, f) => runWith(self, (pull) => forever(flatMap(pull, f), { disableYield: true })));
// node_modules/effect/dist/internal/stream.js
const TypeId = "~effect/Stream";
const streamVariance = {
	_R: identity,
	_E: identity,
	_A: identity
};
const Stream = function(channel) {
	this.channel = channel;
};
Stream.prototype = {
	[TypeId]: streamVariance,
	pipe() {
		return pipeArguments(this, arguments);
	}
};
/** @internal */
const fromChannel$1 = (channel) => new Stream(channel);
// node_modules/effect/dist/Stream.js
/**
* Creates a stream from a array-emitting `Channel`.
*
* **Example** (Creating a stream from an array-emitting channel)
*
* ```ts import.meta.vitest
* import { Channel, Effect, Stream } from "effect"
*
* const channel = Channel.succeed([1, 2, 3] as const)
* const stream = Stream.fromChannel(channel)
* await Effect.runPromise(Stream.runCollect(stream)) // => [1, 2, 3]
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromChannel = fromChannel$1;
/**
* Creates a stream that pulls values from a `Queue.Dequeue`.
*
* **Details**
*
* The stream emits non-empty batches of queued values and ends when the queue
* fails with `Cause.Done`; other queue failures are propagated.
*
* **Example** (Creating a stream from a queue of values)
*
* ```ts import.meta.vitest
* import { Cause, Effect, Queue, Stream } from "effect"
*
* const program = Effect.gen(function*() {
*   const queue = yield* Queue.unbounded<number, Cause.Done>()
*   yield* Queue.offer(queue, 1)
*   yield* Queue.offer(queue, 2)
*   yield* Queue.offer(queue, 3)
*   yield* Queue.end(queue)
*
*   const stream = Stream.fromQueue(queue)
*   const values = yield* Stream.runCollect(stream)
*   values // => [1, 2, 3]
* })
*
* await Effect.runPromise(program)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromQueue = (queue) => fromChannel(fromQueueArray(queue));
/**
* Creates a stream from a subscription to a `PubSub`.
*
* **Example** (Creating a stream from a subscription to a PubSub)
*
* ```ts import.meta.vitest
* import { Effect, Fiber, PubSub, Stream } from "effect"
*
* const program = Effect.gen(function*() {
*   const pubsub = yield* PubSub.unbounded<number>({ replay: 3 })
*
*   const fiber = yield* Stream.fromPubSub(pubsub).pipe(
*     Stream.take(3),
*     Stream.runCollect,
*     Effect.forkChild
*   )
*
*   yield* PubSub.publish(pubsub, 1)
*   yield* PubSub.publish(pubsub, 2)
*   yield* PubSub.publish(pubsub, 3)
*
*   const values = yield* Fiber.join(fiber)
*   values // => [1, 2, 3]
* })
*
* await Effect.runPromise(program)
* ```
*
* @stability stable
* @category constructors
* @since 2.0.0
*/
const fromPubSub = (pubsub) => fromChannel(fromPubSubArray(pubsub));
/**
* Runs the provided effectful callback for each element of the stream.
*
* **Example** (Running an effect for each value)
*
* ```ts import.meta.vitest
* import { Effect, Stream } from "effect"
*
* const stream = Stream.make(1, 2, 3)
* const values: Array<string> = []
*
* const program = Effect.gen(function*() {
*   yield* Stream.runForEach(stream, (n) => Effect.sync(() => values.push(`Processing: ${n}`)))
* })
*
* await Effect.runPromise(program)
* values // => ["Processing: 1", "Processing: 2", "Processing: 3"]
* ```
*
* @stability stable
* @category destructors
* @since 2.0.0
*/
const runForEach = /*#__PURE__*/ dual(2, (self, f) => runForEach$1(self.channel, (arr) => {
	let i = 0;
	return whileLoop({
		while: () => i < arr.length,
		body: () => f(arr[i++]),
		step: constVoid
	});
}));
// src/io.mjs
async function atomic(file, value) {
	await mkdir(dirname(file), {
		recursive: true,
		mode: 448
	});
	const temp = `${file}.${randomUUID()}`;
	const handle = await open(temp, "wx", 384);
	try {
		await handle.writeFile(JSON.stringify(value));
		await handle.sync();
	} finally {
		await handle.close();
	}
	await rename(temp, file);
	const dir = await open(dirname(file), "r");
	try {
		await dir.sync();
	} finally {
		await dir.close();
	}
}
function command(executable, args, { input, timeout = 12e4, signal } = {}) {
	return new Promise((resolve, reject) => {
		const child = spawn(executable, args, {
			stdio: [
				"pipe",
				"pipe",
				"pipe"
			],
			signal,
			timeout,
			env: {
				...process.env,
				BOAT_NO_UPDATE: "1"
			}
		});
		const chunks = [];
		let size = 0, overflow = false;
		child.stdout.on("data", (chunk) => {
			size += chunk.length;
			if (size <= 8388608) chunks.push(chunk);
			else {
				overflow = true;
				child.kill();
			}
		});
		child.stderr.resume();
		child.on("error", () => reject(/* @__PURE__ */ new Error(`${executable} could not start`)));
		child.on("close", (code) => code === 0 && !overflow ? resolve(Buffer.concat(chunks).toString().trim()) : reject(/* @__PURE__ */ new Error(`${executable} failed (${code ?? "terminated"})`)));
		child.stdin.on("error", () => {});
		child.stdin.end(input);
	});
}
const quote = (value) => "'" + String(value).replaceAll("'", "'\\''") + "'";
function safeUrl(value) {
	const u = new URL(value);
	if (!["https:", "http:"].includes(u.protocol) || u.username || u.password || u.search || u.hash) throw new Error("Use an HTTP(S) origin without credentials");
	return u.origin;
}
// src/drivers.mjs
async function transfer(run, ref, path, content, kind) {
	const dir = await mkdtemp(join(tmpdir(), "sandbox-upload-"));
	try {
		const file = join(dir, "payload");
		await writeFile(file, content, { mode: 384 });
		await run(kind, kind === "boat" ? [
			"--no-update",
			"scp",
			file,
			`${ref}:${path}`
		] : [
			"cp",
			file,
			`${ref}:${path}`
		]);
	} finally {
		await rm(dir, {
			recursive: true,
			force: true
		});
	}
}
const parse = JSON.parse;
function boatResult(text) {
	try {
		return JSON.parse(text);
	} catch {
		const frames = text.split("\n").filter((x) => x.trim()).map((x) => JSON.parse(x));
		const last = frames.at(-1);
		return {
			...last,
			...!last.id ? { id: frames.find((x) => x.id)?.id } : {},
			...last.state ? {} : { state: last.event }
		};
	}
}
const state = (value) => ({
	archived: "stopped",
	idle: "running",
	busy: "running",
	ready: "running",
	pending: "starting",
	destroyed: "deleted"
})[value] ?? value;
function driver(profile, run = command) {
	if (profile.driver === "external") return external(profile, run);
	if (profile.driver === "boat") {
		const boat = async (...args) => boatResult(await run("boat", [
			"--no-update",
			"--json",
			...args
		]));
		const observe = (x) => ({
			ref: x.id,
			state: x.dataExpiredAt ? "deleted" : state(x.state),
			name: x.name,
			deadline: x.archiveAfter ? Date.parse(x.archiveAfter) : null
		});
		return {
			capabilities: {
				deadline: true,
				stopKeepsDisk: true
			},
			async list() {
				const result = await boat("list", "--all");
				if (result.pageInfo?.hasMore) throw new Error("Boat listing is incomplete; refusing reconciliation");
				return result.sandboxes.map(observe);
			},
			async create(spec) {
				const x = await boat("new", "--name", spec.name, "--from", profile.image, "--ttl", String(spec.ttl), ...profile.environment ? ["--environment", profile.environment] : ["--no-env"]);
				return observe(x.sandbox ?? x);
			},
			async get(ref) {
				const x = await boat("info", ref);
				return observe(x.sandbox ?? x);
			},
			async start(ref) {
				await boat("resume", ref);
			},
			async stop(ref) {
				await boat("stop", ref);
			},
			async remove(ref) {
				await boat("delete", ref, "--yes");
			},
			async setDeadline(ref, seconds) {
				await boat("extend", ref, "--ttl", String(Math.max(60, seconds)));
			},
			async exec(ref, script) {
				const x = await boat("exec", ref, "--timeout", "600", "--", "bash", "-lc", script);
				if (x.exitCode !== 0 && x.exitCode !== void 0) throw new Error("Boat command failed");
				return x.stdout ?? x.output ?? "";
			},
			async write(ref, path, content) {
				await transfer(run, ref, path, content, "boat");
			},
			async expose(ref) {
				const x = await boat("host", ref, "3773", "--public");
				return safeUrl(x.url ?? x.host?.url);
			}
		};
	}
	if (profile.driver !== "docker") throw new Error("Unknown driver");
	const docker = (...args) => run("docker", args);
	const observe = (x) => ({
		ref: x.Id,
		state: x.State.Running ? "running" : x.State.Status === "created" ? "starting" : "stopped",
		name: x.Name.replace(/^\//, "")
	});
	return {
		capabilities: {
			deadline: false,
			stopKeepsDisk: true
		},
		async list() {
			const ids = (await docker("ps", "-aq", "--filter", "label=t3-sandboxes=true")).split(/\s+/).filter(Boolean);
			return ids.length ? parse(await docker("inspect", ...ids)).map(observe) : [];
		},
		async create(spec) {
			return {
				ref: await docker("run", "-d", "--name", spec.name, "--label", "t3-sandboxes=true", "--init", "-p", "127.0.0.1::3773", profile.image, "sleep", "infinity"),
				state: "running",
				name: spec.name
			};
		},
		async get(ref) {
			return observe(parse(await docker("inspect", ref))[0]);
		},
		async start(ref) {
			await docker("start", ref);
		},
		async stop(ref) {
			await docker("stop", ref);
		},
		async remove(ref) {
			await docker("rm", ref);
		},
		async setDeadline() {},
		async exec(ref, script) {
			return docker("exec", ref, "bash", "-lc", script);
		},
		async write(ref, path, content) {
			await transfer(run, ref, path, content, "docker");
		},
		async expose(ref) {
			return `http://127.0.0.1:${parse(await docker("inspect", ref))[0].NetworkSettings.Ports["3773/tcp"][0].HostPort}`;
		}
	};
}
function external(profile, run) {
	const call = async (operation, input) => {
		if (profile.executable) return JSON.parse(await run(profile.executable, [], { input: JSON.stringify({
			operation,
			input
		}) }));
		const response = await fetch(profile.endpoint, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				...profile.token ? { Authorization: `Bearer ${profile.token}` } : {}
			},
			body: JSON.stringify({
				operation,
				input
			}),
			signal: AbortSignal.timeout(12e4)
		});
		if (!response.ok) throw new Error(`Driver operation failed (${response.status})`);
		const text = await response.text();
		if (text.length > 1048576) throw new Error("Driver result too large");
		return JSON.parse(text);
	};
	return {
		capabilities: profile.capabilities,
		...Object.fromEntries([
			"list",
			"get",
			"start",
			"stop",
			"remove",
			"setDeadline",
			"exec",
			"expose",
			"create",
			"write"
		].map((op) => [op, (...args) => call(op, args)]))
	};
}
function bootstrap(row, profile) {
	const root = "/home/user/t3-sandbox";
	const q = quote;
	return `set -eu
umask 077
mkdir -p ${root}/home/userdata ${root}/plugin
printf %s ${q(JSON.stringify({ plugins: [{
		id: "sandboxes",
		path: root + "/plugin/plugin.mjs",
		enabled: true
	}, ...profile.rehomePath ? [{
		id: "rehome",
		path: profile.rehomePath,
		enabled: true
	}] : []] }))} > ${root}/home/plugins.json
printf %s ${q(JSON.stringify({
		mode: "guest",
		sandboxId: row.id,
		controllerUrl: profile.controllerUrl ?? null,
		key: row.reporterKey
	}))} > ${root}/home/plugins-guest.json
chmod 600 ${root}/home/plugins-guest.json
if [ ! -d ${root}/repo/.git ]; then
  git clone -- ${q(row.repository)} ${root}/repo
  cd ${root}/repo
  git checkout --detach ${q(row.base)}
fi
cd ${root}/repo
# A dedicated image supplies the same plugin host build as the controller.
${quote(profile.t3Command ?? "t3")} --version >/dev/null
if [ ! -f ${root}/server.pid ] || ! kill -0 "$(cat ${root}/server.pid)" 2>/dev/null; then
  T3_REHOME_TRANSFER_KEY="$(node -p 'JSON.parse(require("fs").readFileSync("/home/user/t3-sandbox/home/plugins-guest.json","utf8")).key')" nohup ${quote(profile.t3Command ?? "t3")} serve --base-dir ${root}/home --host 0.0.0.0 --port 3773 --no-browser > ${root}/server.log 2>&1 < /dev/null &
  echo $! > ${root}/server.pid
fi
mkdir -p ${root}/home/plugins/sandboxes
${quote(profile.t3Command ?? "t3")} auth mcp issue --base-dir ${root}/home --access full-access --token-only > ${root}/home/plugins/sandboxes/controller-token
chmod 600 ${root}/home/plugins/sandboxes/controller-token
node -e ${q("const fs=require('node:fs');const file='/home/user/t3-sandbox/server.log';const watcher=fs.watch('/home/user/t3-sandbox',check);const timer=setTimeout(()=>{watcher.close();process.exit(1)},60000);function check(){if(fs.existsSync(file)&&fs.readFileSync(file,'utf8').includes('T3 Code server is ready.')){clearTimeout(timer);watcher.close();}}check();")}
cat ${root}/home/plugins/sandboxes/controller-token`;
}
function pairingScript(profile, url) {
	return `${quote(profile.t3Command ?? "t3")} auth pairing create --base-dir /home/user/t3-sandbox/home --json --base-url ${quote(url)}`;
}
// src/mcp.mjs
var Guest = class {
	constructor(url, token, fetcher = fetch) {
		this.url = new URL("/mcp", url);
		this.token = token;
		this.fetcher = fetcher;
		this.id = 0;
	}
	async rpc(method, params) {
		const response = await this.fetcher(this.url, {
			method: "POST",
			headers: {
				Authorization: `Bearer ${this.token}`,
				"Content-Type": "application/json",
				Accept: "application/json, text/event-stream",
				...this.session ? { "Mcp-Session-Id": this.session } : {}
			},
			body: JSON.stringify({
				jsonrpc: "2.0",
				id: ++this.id,
				method,
				params
			}),
			signal: AbortSignal.timeout(3e4)
		});
		if (!response.ok) throw new Error(`Guest request failed (${response.status})`);
		this.session = response.headers.get("mcp-session-id") ?? this.session;
		const text = await response.text();
		if (text.length > 8388608) throw new Error("Guest response too large");
		const message = (response.headers.get("content-type")?.includes("text/event-stream") ? text.split("\n").filter((x) => x.startsWith("data:")).map((x) => JSON.parse(x.slice(5))) : [JSON.parse(text)]).find((x) => x.id === this.id);
		if (!message || message.error) throw new Error("Guest RPC failed");
		return message.result;
	}
	async call(name, args = {}) {
		if (!this.initialized) {
			await this.rpc("initialize", {
				protocolVersion: "2025-03-26",
				capabilities: {},
				clientInfo: {
					name: "t3-sandboxes",
					version: "0.1.0"
				}
			});
			this.initialized = true;
		}
		const result = await this.rpc("tools/call", {
			name,
			arguments: args
		});
		if (result.isError) throw new Error(`Guest tool failed: ${name}`);
		return result.structuredContent ?? JSON.parse(result.content.find((x) => x.type === "text").text);
	}
	async threads() {
		const all = [];
		let cursor;
		do {
			const page = await this.call("t3_thread_list", {
				limit: 100,
				allProjects: true,
				includeSubagents: true,
				...cursor === void 0 ? {} : { cursor }
			});
			all.push(...page.threads);
			cursor = page.nextCursor;
		} while (cursor !== null && cursor !== void 0);
		return all;
	}
	async activity() {
		return this.call("t3_plugin_read", {
			pluginId: "sandboxes",
			commandId: "activity",
			input: {},
			permission: "orchestration:read"
		});
	}
	async transcript(threadId) {
		const items = [];
		let afterPosition;
		do {
			const page = await this.call("t3_thread_read", {
				threadId,
				view: "activity",
				limit: 50,
				maxCharsPerItem: 5e4,
				...afterPosition === void 0 ? {} : { afterPosition }
			});
			for (const item of page.items) {
				let full = item;
				while (full.nextTextOffset !== null && full.nextTextOffset !== void 0) {
					const part = (await this.call("t3_thread_read", {
						threadId,
						itemId: item.itemId,
						textOffset: full.nextTextOffset,
						maxCharsPerItem: 5e4
					})).items[0];
					full = {
						...full,
						text: (full.text ?? "") + (part.text ?? ""),
						nextTextOffset: part.nextTextOffset
					};
				}
				items.push(full);
			}
			afterPosition = page.hasMore ? page.nextPosition : null;
		} while (afterPosition !== null);
		return {
			threadId,
			items
		};
	}
};
// src/policy.mjs
const defaults = Object.freeze({
	idleMinutes: 15,
	waitingMinutes: 60,
	unattendedMinutes: 120,
	continuousMinutes: 720,
	deleteDays: 7,
	maxRunning: 3,
	monthlyBudget: 0
});
function policy(input = {}) {
	const result = {
		...defaults,
		...input
	};
	for (const [key, value] of Object.entries(result)) if (!Object.hasOwn(defaults, key) || !Number.isFinite(value) || value < 0 || value > 525600) throw new Error(`Invalid policy: ${key}`);
	for (const key of [
		"idleMinutes",
		"waitingMinutes",
		"unattendedMinutes",
		"continuousMinutes",
		"maxRunning"
	]) if (result[key] < 1) throw new Error(`Invalid policy: ${key}`);
	return result;
}
function decide(row, activity, now, p) {
	if (row.desired !== "running") return row.desired;
	if (now >= row.startedAt + p.continuousMinutes * 6e4) return "stopped";
	if (row.keepAwakeUntil > now) return "running";
	if (!activity) return "unknown";
	if (activity.closed) return "stopped";
	if (activity.active) return "running";
	if (activity.waiting) return now >= (row.waitingSince ?? now) + p.waitingMinutes * 6e4 ? "stopped" : "running";
	return now >= Math.max(row.lastActivity, activity.lastActivity ?? 0) + p.idleMinutes * 6e4 ? "stopped" : "running";
}
function deadline(row, activity, now, p) {
	const hard = row.startedAt + p.continuousMinutes * 6e4;
	const next = row.keepAwakeUntil > now ? row.keepAwakeUntil : activity?.active ? now + p.unattendedMinutes * 6e4 : activity?.waiting ? (row.waitingSince ?? now) + p.waitingMinutes * 6e4 : row.lastActivity + p.idleMinutes * 6e4;
	return Math.min(hard, next);
}
function tone(state) {
	return state === "running" ? "success" : state === "stopped" ? "muted" : [
		"starting",
		"stopping",
		"deleting"
	].includes(state) ? "warning" : "danger";
}
// src/billing.mjs
function usage(rows, profiles, now) {
	const monthStart = new Date(now);
	monthStart.setUTCDate(1);
	monthStart.setUTCHours(0, 0, 0, 0);
	const start = +monthStart;
	const items = rows.map((row) => {
		const rate = row.hourlyRate ?? profiles.find((p) => p.id === row.profile)?.hourlyRate ?? 0;
		const spans = row.sessions ?? [{
			start: row.startedAt,
			end: row.stoppedAt ?? null
		}];
		const seconds = spans.reduce((total, span) => total + Math.max(0, (span.end ?? (row.enforcedDeadline ? Math.min(now, row.deadline ?? now) : now)) - span.start) / 1e3, 0);
		const monthSeconds = spans.reduce((total, span) => total + Math.max(0, (span.end ?? (row.enforcedDeadline ? Math.min(now, row.deadline ?? now) : now)) - Math.max(start, span.start)) / 1e3, 0);
		return {
			id: row.id,
			title: row.title,
			seconds,
			cost: seconds * rate / 3600,
			monthCost: monthSeconds * rate / 3600,
			rateKnown: rate > 0
		};
	});
	return {
		items,
		monthCost: items.reduce((total, item) => total + item.monthCost, 0)
	};
}
// src/controller.mjs
async function createController({ directory, secrets, source, changed = () => {}, diagnostic = () => {}, preserve = async () => false, drivers = driver, guest = (url, token) => new Guest(url, token), now = Date.now }) {
	await mkdir(directory, {
		recursive: true,
		mode: 448
	});
	const file = join(directory, "ledger.json");
	let state;
	try {
		state = JSON.parse(await readFile(file, "utf8"));
		if (state.version !== 1 || !Array.isArray(state.rows) || !Array.isArray(state.profiles)) throw new Error("Invalid ledger");
	} catch (error) {
		if (error.code !== "ENOENT") throw error;
		state = {
			version: 1,
			owner: randomUUID(),
			policy: policy(),
			profiles: [],
			rows: []
		};
		await atomic(file, state);
	}
	let tail = Promise.resolve();
	let saveTail = Promise.resolve();
	const workers = /* @__PURE__ */ new Map();
	let closed = false;
	const serial = (fn) => {
		const result = tail.then(() => {
			if (closed) throw new Error("Controller closed");
			return fn();
		});
		tail = result.catch(() => {});
		return result;
	};
	const save = async () => {
		const snapshot = structuredClone(state);
		saveTail = saveTail.then(() => atomic(file, snapshot));
		await saveTail;
		changed();
	};
	const profile = (id) => {
		const p = state.profiles.find((x) => x.id === id);
		if (!p) throw new Error("Missing profile");
		return p;
	};
	const getDriver = async (row) => {
		const p = profile(row.profile);
		return drivers({
			...p,
			token: await secrets.get(`provider-${p.id}`)
		});
	};
	const getGuest = async (row) => guest(row.url, await secrets.get(`guest-${row.id}`));
	async function retain(row) {
		const g = await getGuest(row);
		let paused = false;
		try {
			await g.call("t3_plugin_run", {
				pluginId: "sandboxes",
				commandId: "prepare-stop",
				input: {},
				permission: "orchestration:operate"
			});
			paused = true;
		} catch {
			row.workSafe = false;
		}
		const threads = await g.threads();
		for (const thread of threads) {
			const transcript = await g.transcript(thread.threadId);
			await atomic(join(directory, "transcripts", row.id, `${Buffer.from(thread.threadId).toString("hex")}.json`), transcript);
		}
		try {
			row.rehomed = await preserve(row, threads);
		} catch {
			row.rehomed = false;
		}
		try {
			row.workSafe = paused && await (await getDriver(row)).exec(row.ref, `cd /home/user/t3-sandbox/repo && git fetch origin >/dev/null 2>&1 && if [ -z "$(git status --porcelain)" ] && [ "$(git rev-list --count HEAD --not --remotes)" = 0 ]; then printf safe; else printf unsafe; fi`) === "safe";
		} catch {
			row.workSafe = false;
		}
		row.retainedAt = now();
		await save();
	}
	async function reconcile(row) {
		const d = await getDriver(row);
		const p = profile(row.profile);
		row.enforcedDeadline = d.capabilities.deadline;
		try {
			if (!row.ref) {
				const matches = (await d.list()).filter((x) => x.name === row.name);
				if (matches.length > 1) throw new Error("Duplicate provider resources require recovery");
				if (matches.length) row.ref = matches[0].ref;
				else if (row.desired !== "running") {
					row.state = "deleted";
					await save();
					return;
				} else {
					if (row.createAttempted) throw new Error("Create outcome unknown; reconcile the provider before retrying");
					row.createAttempted = true;
					await save();
					const made = await d.create({
						name: row.name,
						requestId: row.id,
						ttl: state.policy.unattendedMinutes * 60
					});
					if (!made.ref) throw new Error("Provider returned no resource id");
					row.ref = made.ref;
					await save();
				}
			}
			row.stage = "Inspecting resource";
			let observed;
			try {
				observed = await d.get(row.ref);
			} catch (cause) {
				if ((await d.list()).some((item) => item.ref === row.ref)) throw cause;
				row.state = "deleted";
				row.desired = "deleted";
				await save();
				return;
			}
			if (row.orphan && row.desired === "stopped") {
				if (observed.state === "running") await d.stop(row.ref);
				row.state = "stopped";
				row.stoppedAt ??= now();
				await save();
				return;
			}
			if (observed.state === "deleted") {
				row.state = "deleted";
				row.desired = "deleted";
				await save();
				return;
			}
			if (observed.state === "stopped" && row.desired === "running" && !row.resumeRequested) {
				row.desired = "stopped";
				row.state = "stopped";
				row.stoppedAt ??= now();
				row.bootstrapped = false;
				row.error = "Provider stopped the sandbox; resume explicitly when ready.";
				if (row.sessions?.at(-1)) row.sessions.at(-1).end = Math.min(now(), row.deadline ?? now());
				await save();
				return;
			}
			if (observed.state === "stopped" && row.desired === "stopped") {
				row.state = "stopped";
				row.stoppedAt ??= now();
				await save();
				return;
			}
			if ([
				"creating",
				"starting",
				"stopping"
			].includes(observed.state)) {
				row.state = observed.state === "creating" ? "starting" : observed.state;
				await save();
				return;
			}
			if (row.desired === "running") {
				row.stage = "Starting sandbox";
				if (observed.state === "stopped") {
					await d.start(row.ref);
					row.resumeRequested = false;
					row.startedAt = now();
					(row.sessions ??= []).push({
						start: now(),
						end: null
					});
					row.lastActivity = now();
					row.bootstrapped = false;
					row.deadline = null;
				}
				if (!row.bootstrapped) {
					row.state = "starting";
					await save();
					const key = await secrets.get(`reporter-${row.id}`) ?? randomBytes(32).toString("hex");
					await secrets.set(`reporter-${row.id}`, key);
					await d.exec(row.ref, "mkdir -p /home/user/t3-sandbox/plugin");
					await d.write(row.ref, "/home/user/t3-sandbox/plugin/plugin.mjs", source);
					row.stage = "Bootstrapping T3";
					const token = await d.exec(row.ref, bootstrap({
						...row,
						reporterKey: key
					}, p));
					await secrets.set(`guest-${row.id}`, String(token).trim());
					row.url = p.url ? safeUrl(p.url.replaceAll("{id}", row.ref)) : await d.expose(row.ref);
					row.bootstrapped = true;
					await save();
				}
				if (row.desired !== "running") {
					await stop(row, d);
					return;
				}
				const g = await getGuest(row);
				row.stage = "Starting thread";
				if (!row.threadId) {
					const project = (await g.call("t3_project_list", { limit: 100 })).projects.find((x) => x.workspaceRoot === "/home/user/t3-sandbox/repo") ?? await g.call("t3_project_create", {
						title: row.title,
						workspaceRoot: "/home/user/t3-sandbox/repo"
					});
					row.threadId = (await g.call("t3_thread_launch", {
						projectId: project.id,
						title: row.title,
						...row.message ? { message: row.message } : {},
						...row.modelSelection ? { modelSelection: row.modelSelection } : {},
						clientRequestId: row.id,
						workspaceStrategy: { type: "root" }
					})).threadId;
					row.message = null;
					await save();
				}
				row.state = "running";
				row.error = null;
				await d.exec(row.ref, `mkdir -p /home/user/t3-sandbox/home/plugins/sandboxes`);
				await d.write(row.ref, "/home/user/t3-sandbox/home/plugins/sandboxes/status.json", JSON.stringify({
					state: row.state,
					detail: row.deadline ? "Provider stop deadline: " + new Date(row.deadline).toISOString() : "Dedicated sandbox environment"
				}));
				let activity;
				try {
					activity = await g.activity();
				} catch {
					activity = null;
				}
				if (activity) {
					if (activity.environmentId) row.environmentId = activity.environmentId;
					if (activity.active) row.lastActivity = now();
					else row.lastActivity = Math.max(row.lastActivity, activity.lastActivity);
					if (activity.waiting) row.waitingSince ??= now();
					else row.waitingSince = null;
					row.activity = activity;
				}
				if (decide(row, activity, now(), state.policy) === "stopped") {
					row.desired = "stopped";
					await save();
					await stop(row, d);
					return;
				}
				if (activity) {
					const until = deadline(row, activity, now(), state.policy);
					if (!row.deadline || until < row.deadline - 3e4 || row.deadline - now() < (until - now()) / 2) {
						await d.setDeadline(row.ref, Math.ceil((until - now()) / 1e3));
						row.deadline = until;
					}
				}
			} else if (row.desired === "stopped") {
				await stop(row, d);
				return;
			} else {
				if (observed.state !== "stopped") {
					row.desired = "stopped";
					await stop(row, d);
					row.desired = "deleted";
					await save();
				}
				if (!row.confirmDelete && !row.autoDeleteSafe) throw new Error("Explicit data-loss confirmation required");
				row.state = "deleting";
				await save();
				await d.remove(row.ref);
				row.state = "deleted";
				await secrets.remove(`guest-${row.id}`);
				await secrets.remove(`reporter-${row.id}`);
			}
			await save();
		} catch (cause) {
			diagnostic(row.stage, cause);
			row.error = `Could not complete ${row.stage ?? "provider operation"}. Check the profile and connectivity; provider data was retained.`;
			if (row.state !== "running") row.state = "error";
			await save();
		}
	}
	async function stop(row, d) {
		if (row.state === "stopped") return;
		row.state = "stopping";
		await save();
		if (row.threadId) await retain(row);
		await d.stop(row.ref);
		row.state = (await d.get(row.ref)).state;
		if (row.state !== "stopped") {
			await save();
			return;
		}
		row.stoppedAt = now();
		if (row.sessions?.at(-1)) row.sessions.at(-1).end = now();
		row.bootstrapped = false;
		await save();
	}
	return {
		snapshot: () => structuredClone(state),
		usage: () => usage(state.rows, state.profiles, now()),
		configure: (input) => serial(async () => {
			const id = String(input.id ?? "");
			if (!/^[a-z][a-z0-9-]{0,30}$/.test(id)) throw new Error("Invalid profile id");
			if (state.rows.some((r) => r.profile === id && r.state !== "deleted")) throw new Error("Stop and remove resources before replacing a profile");
			if (![
				"boat",
				"docker",
				"external"
			].includes(input.driver)) throw new Error("Invalid driver");
			if (input.driver !== "external" && (!input.image || typeof input.image !== "string")) throw new Error("Image or snapshot required");
			const p = {
				id,
				driver: input.driver,
				image: input.image,
				environment: input.environment || void 0,
				url: input.url || void 0,
				controllerUrl: input.controllerUrl ? safeUrl(input.controllerUrl) : void 0,
				t3Command: input.t3Command || "t3",
				executable: input.executable || void 0,
				endpoint: input.endpoint ? safeUrl(input.endpoint) : void 0,
				rehomePath: input.rehomePath || void 0,
				hourlyRate: Number(input.hourlyRate ?? 0),
				capabilities: input.capabilities ?? {
					deadline: false,
					stopKeepsDisk: true
				}
			};
			if (!Number.isFinite(p.hourlyRate) || p.hourlyRate < 0) throw new Error("Invalid hourly rate");
			state.profiles = state.profiles.filter((x) => x.id !== id).concat(p);
			if (input.token) await secrets.set(`provider-${id}`, input.token);
			await save();
		}),
		setPolicy: (input) => serial(async () => {
			state.policy = policy(input);
			await save();
		}),
		create: (input) => serial(async () => {
			const id = input.requestId ?? randomUUID();
			if (!/^[a-f0-9-]{36}$/.test(id)) throw new Error("Invalid request id");
			const old = state.rows.find((r) => r.id === id);
			if (old) return old.id;
			const p = profile(input.profile);
			if (state.rows.filter((r) => r.desired === "running" && r.state !== "deleted").length >= state.policy.maxRunning) throw new Error("Running sandbox limit reached");
			if (state.policy.monthlyBudget > 0) {
				if (!(p.hourlyRate > 0)) throw new Error("Configure an hourly rate before using a budget");
				if (usage(state.rows, state.profiles, now()).monthCost >= state.policy.monthlyBudget) throw new Error("Monthly estimated budget reached");
			}
			if (typeof input.repository !== "string" || !/^https:\/\/[^\s]+$|^git@[^\s]+:[^\s]+$/.test(input.repository)) throw new Error("Use a Git remote");
			if (typeof input.base !== "string" || !/^[a-f0-9]{40}$/.test(input.base)) throw new Error("Use the full pushed commit SHA");
			const row = {
				id,
				name: `t3-${state.owner.slice(0, 8)}-${p.id}-${id}`,
				profile: p.id,
				title: String(input.title ?? "Sandbox").slice(0, 150),
				hourlyRate: p.hourlyRate,
				repository: input.repository,
				base: input.base,
				message: input.message ?? null,
				modelSelection: input.modelSelection ?? null,
				state: "starting",
				desired: "running",
				startedAt: now(),
				sessions: [{
					start: now(),
					end: null
				}],
				lastActivity: now(),
				pinned: false
			};
			state.rows.push(row);
			await save();
			return row.id;
		}),
		adopt: (input) => serial(async () => {
			const row = state.rows.find((r) => r.id === input.id && r.orphan);
			if (!row) throw new Error("Unknown orphan");
			if (typeof input.repository !== "string" || !/^https:\/\/[^\s]+$|^git@[^\s]+:[^\s]+$/.test(input.repository) || !/^[a-f0-9]{40}$/.test(input.base)) throw new Error("Supply a Git remote and full pushed base SHA");
			row.repository = input.repository;
			row.base = input.base;
			row.orphan = false;
			row.title = input.title ?? "Adopted sandbox";
			row.desired = "stopped";
			await save();
		}),
		action: (id, action, input = {}) => serial(async () => {
			const row = state.rows.find((r) => r.id === id);
			if (!row) throw new Error("Unknown sandbox");
			if (row.orphan && action === "running") throw new Error("Adopt the orphan with a repository and base before resuming.");
			if (action === "keep-awake") {
				if (!Number.isFinite(input.hours) || input.hours < 0 || input.hours > 24) throw new Error("Keep awake must be 0–24 hours");
				row.keepAwakeUntil = now() + input.hours * 36e5;
			} else if (action === "pin") row.pinned = input.pinned === true;
			else if ([
				"running",
				"stopped",
				"deleted"
			].includes(action)) {
				if (action === "deleted" && input.confirm !== true) throw new Error("Confirm permanent loss of workspace and thread data");
				if (action === "running") {
					if (row.state === "deleted") throw new Error("Deleted sandboxes cannot be resumed.");
					if (state.rows.filter((r) => r.id !== row.id && r.desired === "running" && r.state !== "deleted").length >= state.policy.maxRunning) throw new Error("Running sandbox limit reached");
					const p = profile(row.profile);
					if (state.policy.monthlyBudget > 0 && (!(p.hourlyRate > 0) || usage(state.rows, state.profiles, now()).monthCost >= state.policy.monthlyBudget)) throw new Error("Monthly estimated budget reached or rate missing");
					row.resumeRequested = true;
				}
				row.desired = action;
				row.confirmDelete = input.confirm === true;
			} else throw new Error("Unknown action");
			await save();
		}),
		pair: (id) => serial(async () => {
			const row = state.rows.find((r) => r.id === id);
			if (!row || row.state !== "running") throw new Error("Resume sandbox first");
			return JSON.parse(await (await getDriver(row)).exec(row.ref, pairingScript(profile(row.profile), row.url))).pairUrl;
		}),
		tick: async () => {
			if (closed) return;
			for (const p of state.profiles) try {
				const d = drivers({
					...p,
					token: await secrets.get(`provider-${p.id}`)
				});
				for (const observed of await d.list()) {
					if (!observed.name?.startsWith(`t3-${state.owner.slice(0, 8)}-${p.id}-`) || state.rows.some((r) => r.ref === observed.ref || r.name === observed.name)) continue;
					state.rows.push({
						id: randomUUID(),
						name: observed.name,
						ref: observed.ref,
						profile: p.id,
						title: "Orphaned sandbox",
						state: observed.state,
						desired: "stopped",
						orphan: true,
						pinned: true,
						startedAt: now(),
						lastActivity: now()
					});
					await save();
				}
			} catch {}
			const pending = [];
			for (const row of state.rows) {
				if (row.state === "stopped" && row.desired === "stopped" && !row.pinned && state.policy.deleteDays > 0 && now() >= row.stoppedAt + state.policy.deleteDays * 864e5) {
					if (row.retainedAt && (row.workSafe || row.rehomed)) {
						row.desired = "deleted";
						row.autoDeleteSafe = true;
						await save();
					} else row.error = "Automatic deletion blocked: work has not been verified as retained or pushed.";
				}
				if (row.state === "deleted" || row.state === "stopped" && row.desired === "stopped") continue;
				let worker = workers.get(row.id);
				if (!worker) {
					worker = reconcile(row).finally(() => workers.delete(row.id));
					workers.set(row.id, worker);
				}
				pending.push(worker);
			}
			await Promise.all(pending);
		},
		close: async () => {
			await tail;
			await Promise.all(workers.values());
			await saveTail;
			closed = true;
		}
	};
}
// src/plugin.mjs
var PluginError = class extends TaggedError()("PluginError", {
	operation: String$1,
	cause: Defect()
}) {};
const call = (operation, run) => tryPromise({
	try: run,
	catch: (cause) => new PluginError({
		operation,
		cause
	})
});
const action = (id, label, command, input = {}) => ({
	id,
	label,
	command,
	input
});
const field = (name, label, kind = "text", value) => ({
	name: name.replace(/[A-Z]/g, (letter) => "-" + letter.toLowerCase()),
	label,
	kind,
	...value === void 0 ? {} : { value }
});
const form = (id, label, command, fields, input = {}) => ({
	id,
	kind: "form",
	label,
	command,
	fields,
	input
});
const descriptor = (id, title, run, { agent = false, permission = "environment:maintain" } = {}) => ({
	id,
	title,
	description: title,
	permission,
	agent,
	run: (input) => call(id, () => run(Object.fromEntries(Object.entries(input).map(([key, value]) => [key.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase()), value]))))
});
function settings(controller) {
	const state = controller.snapshot();
	return {
		title: "Sandboxes",
		blocks: [
			{
				id: "intro",
				kind: "text",
				text: "Each sandbox runs its own T3 environment. Use an image or Boat snapshot containing this plugin host build and your chosen agent tools. Start a sandbox here, then pair to open its thread. Docker is reachable from this computer only; use a provider HTTPS URL or a tailnet URL for your phone."
			},
			{
				id: "resources",
				kind: "table",
				columns: [
					"Thread",
					"ID",
					"Profile",
					"State",
					"Retention"
				],
				rows: state.rows.slice(-500).map((r) => ({
					id: r.id,
					cells: [
						r.title ?? r.name,
						r.id,
						r.profile,
						r.error ?? r.state,
						r.retainedAt ? "Transcript saved locally" : "Not exported yet"
					],
					actions: r.state === "deleted" ? [] : [
						action("stop", "Stop", "state", {
							id: r.id,
							state: "stopped"
						}),
						action("resume", "Resume", "state", {
							id: r.id,
							state: "running"
						}),
						action("pair", "Pair client", "pair", { id: r.id }),
						action("pin", r.pinned ? "Unpin" : "Pin", "pin", {
							id: r.id,
							pinned: !r.pinned
						})
					]
				}))
			},
			form("profile", "Save provider profile", "profile", [
				field("id", "Profile name"),
				{
					name: "driver",
					label: "Driver",
					kind: "select",
					options: [
						"docker",
						"boat",
						"external"
					].map((value) => ({
						label: value,
						value
					}))
				},
				field("image", "Docker image or Boat snapshot"),
				field("environment", "Boat environment (blank injects no credentials)"),
				field("url", "Tailnet URL override ({id} is replaced)"),
				field("controllerUrl", "Reachable controller URL (optional, for guest controls)"),
				field("t3Command", "T3 executable in image", "text", "t3"),
				field("executable", "External driver executable"),
				field("rehomePath", "Optional Rehome plugin path inside guest image"),
				field("hourlyRate", "Hourly rate for estimated cost and budget", "number", 0),
				field("endpoint", "External driver HTTPS endpoint"),
				field("token", "External driver bearer key", "password")
			]),
			form("create", "Create sandbox and start thread", "create", [
				{
					name: "profile",
					label: "Provider profile",
					kind: "select",
					options: state.profiles.map((profile) => ({
						label: profile.id,
						value: profile.id
					}))
				},
				field("repository", "Pushed Git remote"),
				field("base", "Full pushed commit SHA"),
				field("title", "Thread title"),
				field("message", "First message (optional)")
			]),
			form("policy", "Save cost policy", "policy", Object.entries(state.policy).map(([key, value]) => field(key, key, "number", value))),
			form("adopt", "Adopt an orphaned sandbox", "adopt", [
				field("id", "Orphan sandbox ID"),
				field("repository", "Git remote"),
				field("base", "Full pushed base commit SHA"),
				field("title", "Thread title")
			]),
			form("awake", "Keep awake or clear", "awake", [field("id", "Sandbox ID"), field("hours", "Hours (0 clears, maximum 24)", "number", 1)]),
			form("delete", "Permanently delete sandbox", "delete", [field("id", "Sandbox ID"), field("confirm", "I understand this permanently deletes the checkout and guest thread data. Transcripts exclude workspace files.", "boolean", false)]),
			{
				id: "stop-all",
				kind: "actions",
				actions: [action("stop-all", "Stop all", "stop-all")]
			},
			{
				id: "retention",
				kind: "text",
				text: "Before a controlled stop, all guest thread transcripts are exported to this plugin’s private data directory. Stop preserves provider disk; Delete permanently removes it. Keep the plugin enabled until every sandbox has stopped. Provider deadlines still apply if the controller is offline. Monthly budget caps refuse creation when spend cannot be verified. Docker has no independent provider deadline."
			}
		]
	};
}
var plugin_default = {
	apiVersion: 1,
	id: "sandboxes",
	name: "Sandboxes",
	version: "0.1.0",
	description: "On-demand thread environments with bounded leases and retained history.",
	activate: (context) => gen(function* () {
		const changes = yield* unbounded();
		const guestConfig = yield* call("guest-config", async () => {
			try {
				return JSON.parse(await readFile(join(context.dataDirectory, "..", "..", "plugins-guest.json"), "utf8"));
			} catch (error) {
				if (error.code === "ENOENT") return null;
				throw error;
			}
		});
		if (guestConfig?.mode === "guest") {
			const status = async () => {
				try {
					return JSON.parse(await readFile(join(context.dataDirectory, "status.json"), "utf8"));
				} catch {
					return {
						state: "running",
						detail: "Controller status unavailable; provider deadline remains in force."
					};
				}
			};
			const remote = async (command, input) => {
				if (!guestConfig.controllerUrl) throw new Error("Manage this sandbox from its controller");
				if (!(await fetch(`${guestConfig.controllerUrl}/api/plugins/sandboxes/control`, {
					method: "POST",
					headers: {
						Authorization: `Bearer ${guestConfig.key}`,
						"Content-Type": "application/json"
					},
					body: JSON.stringify({
						id: guestConfig.sandboxId,
						command,
						input
					}),
					signal: AbortSignal.timeout(15e3)
				})).ok) throw new Error("Controller unavailable");
				return null;
			};
			yield* acquireRelease(sync(() => watch(context.dataDirectory, () => {
				runFork(publish(changes, void 0));
			})), (watcher) => sync(() => watcher.close()));
			return {
				changes: fromPubSub(changes),
				commands: [
					descriptor("prepare-stop", "Pause sandbox threads and capture checkpoints", async () => {
						if (!context.threads) throw new Error("Thread portability host capability required");
						const threads = await new Guest("http://127.0.0.1:3773", (await readFile(join(context.dataDirectory, "controller-token"), "utf8")).trim()).threads();
						for (const thread of threads.filter((t) => t.relationshipToParent !== "subagent")) await runPromise(context.threads.export({
							threadId: thread.threadId,
							file: join(context.dataDirectory, `pause-${Buffer.from(thread.threadId).toString("hex")}.json`)
						}));
						return null;
					}, {
						agent: true,
						permission: "orchestration:operate"
					}),
					descriptor("activity", "Read committed sandbox activity", async () => ({
						...await runPromise(context.activity),
						...context.environmentId ? { environmentId: await runPromise(context.environmentId) } : {}
					}), {
						agent: true,
						permission: "orchestration:read"
					}),
					descriptor("stop", "Stop sandbox", () => remote("stopped", {})),
					descriptor("awake", "Keep sandbox awake", (input) => remote("keep-awake", input), { agent: true })
				],
				views: [{
					id: "status",
					title: "Sandbox",
					location: "thread-details",
					permission: "orchestration:read",
					read: () => call("status", async () => {
						const s = await status();
						return {
							title: "Sandbox",
							blocks: [{
								id: "sandbox",
								kind: "status",
								tone: tone(s.state),
								label: s.state,
								detail: s.detail ?? "Dedicated sandbox environment",
								actions: guestConfig.controllerUrl ? [{
									...action("stop", "Stop sandbox", "stop"),
									confirm: "Stop this sandbox? Its thread transcripts will be retained by the controller."
								}] : []
							}]
						};
					})
				}]
			};
		}
		const queue = yield* unbounded$1();
		const notify = () => {
			runFork(publish(changes, void 0));
		};
		const secrets = Object.fromEntries([
			"get",
			"set",
			"remove"
		].map((key) => [key, (...args) => runPromise(context.secrets[key](...args))]));
		const controller = yield* call("activate", async () => createController({
			directory: context.dataDirectory,
			secrets,
			source: await readFile(fileURLToPath(import.meta.url), "utf8"),
			changed: notify,
			preserve: async (row, threads) => {
				if (!(context.plugins ? await runPromise(context.plugins.list()) : []).some((p) => p.id === "rehome" && p.commands.some((c) => c.id === "pull"))) return false;
				if (!controller.snapshot().profiles.find((p) => p.id === row.profile)?.rehomePath) return false;
				const key = await secrets.get(`reporter-${row.id}`);
				for (const thread of threads.filter((t) => t.relationshipToParent !== "subagent")) {
					let result = await runPromise(context.plugins.run({
						pluginId: "rehome",
						commandId: "pull",
						input: {
							threadId: thread.threadId,
							source: {
								url: row.url,
								key
							},
							waitMs: 2e4
						}
					}));
					while (result.ok && result.state === "running") result = await runPromise(context.plugins.run({
						pluginId: "rehome",
						commandId: "status",
						input: {
							moveId: result.moveId,
							waitMs: 2e4
						}
					}));
					if (!result.ok || result.state !== "done") return false;
				}
				return true;
			}
		}));
		yield* addFinalizer(() => call("close", () => controller.close()).pipe(ignore));
		yield* fromQueue(queue).pipe(runForEach(() => call("reconcile", () => controller.tick()).pipe(ignore)), forkScoped);
		yield* offer(queue, void 0).pipe(repeat(spaced("30 seconds")), forkScoped);
		const mutate = async (fn) => {
			const result = await fn();
			runFork(offer(queue, void 0));
			return result ?? null;
		};
		return {
			beforeRemove: () => call("before-remove", async () => {
				if (controller.snapshot().rows.some((row) => row.state !== "deleted")) throw new Error("Stop and delete managed sandboxes before disabling or removing this plugin.");
				const archive = join(context.dataDirectory, "..", "..", "sandbox-archives");
				await mkdir(archive, {
					recursive: true,
					mode: 448
				});
				try {
					await cp(join(context.dataDirectory, "transcripts"), archive, {
						recursive: true,
						force: false
					});
				} catch (error) {
					if (error.code !== "ENOENT") throw error;
				}
			}),
			changes: fromPubSub(changes),
			commands: [
				descriptor("profile", "Configure provider profile", (input) => controller.configure(input)),
				descriptor("adopt", "Adopt orphaned sandbox", (input) => controller.adopt(input)),
				descriptor("policy", "Configure sandbox policy", (input) => controller.setPolicy(input)),
				descriptor("create", "Create sandbox", (input) => mutate(() => controller.create(input))),
				descriptor("list", "List sandbox states", async () => controller.snapshot().rows.map(({ id, state, title, profile }) => ({
					id,
					state,
					title,
					profile
				})), {
					agent: true,
					permission: "orchestration:read"
				}),
				descriptor("state", "Stop or resume sandbox", (input) => mutate(() => controller.action(input.id, input.state))),
				descriptor("awake", "Keep sandbox awake or clear", (input) => mutate(() => controller.action(input.id, "keep-awake", input)), { agent: true }),
				descriptor("pin", "Pin or unpin sandbox", (input) => mutate(() => controller.action(input.id, "pin", input))),
				descriptor("delete", "Permanently delete sandbox", (input) => mutate(() => controller.action(input.id, "deleted", input))),
				descriptor("pair", "Issue one-time sandbox pairing URL", async (input) => ({
					$t3: "secret",
					label: "Sandbox pairing URL",
					value: await controller.pair(input.id)
				})),
				descriptor("stop-all", "Stop all sandboxes", async () => mutate(async () => {
					for (const r of controller.snapshot().rows) if (r.state !== "deleted") await controller.action(r.id, "stopped");
				}))
			],
			views: [
				{
					id: "managed-status",
					title: "Managed sandboxes",
					location: "thread-details",
					permission: "orchestration:read",
					read: () => succeed$1({
						title: "Managed sandboxes",
						blocks: controller.snapshot().rows.filter((row) => row.environmentId).slice(-100).map((row) => ({
							id: row.id.replace(/^[0-9]/, "s$&"),
							kind: "status",
							environmentId: row.environmentId,
							tone: tone(row.state),
							label: `${row.profile}: ${row.state}`,
							detail: row.error ?? "Managed sandbox",
							actions: row.state === "deleted" ? [] : [{
								...action("state", row.state === "stopped" ? "Resume" : "Stop", "state", {
									id: row.id,
									state: row.state === "stopped" ? "running" : "stopped"
								}),
								...row.state === "stopped" ? {} : { confirm: "Stop the sandbox after retaining its thread history?" }
							}]
						}))
					})
				},
				{
					id: "settings",
					title: "Sandboxes",
					location: "settings-page",
					permission: "environment:maintain",
					read: () => succeed$1(settings(controller))
				},
				{
					id: "usage",
					title: "Sandbox usage",
					location: "usage",
					permission: "orchestration:read",
					read: () => succeed$1({
						title: "Sandbox usage",
						blocks: [{
							id: "cost",
							kind: "text",
							text: `Estimated month total: $${controller.usage().monthCost.toFixed(2)}. Rates are configured per profile; provider invoices are authoritative.`
						}, {
							id: "resources",
							kind: "table",
							columns: [
								"Thread",
								"Runtime hours",
								"Estimated cost"
							],
							rows: controller.usage().items.map((item) => ({
								id: item.id,
								cells: [
									item.title ?? item.id,
									(item.seconds / 3600).toFixed(2),
									item.rateKnown ? "$" + item.cost.toFixed(2) : "Rate not configured"
								],
								actions: []
							}))
						}]
					})
				}
			],
			handleHttp: (request) => call("control", async () => {
				if (request.method !== "POST" || new URL(request.url).pathname.split("/").at(-1) !== "control") return new Response(null, { status: 404 });
				const text = await request.text();
				if (text.length > 8192) return new Response(null, { status: 413 });
				const body = JSON.parse(text);
				if (!/^[a-f0-9-]{36}$/.test(body.id)) return new Response(null, { status: 400 });
				const expected = await secrets.get(`reporter-${body.id}`);
				const supplied = request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
				if (!expected || Buffer.byteLength(expected) !== Buffer.byteLength(supplied) || !timingSafeEqual(Buffer.from(expected), Buffer.from(supplied))) return new Response(null, { status: 401 });
				if (!["stopped", "keep-awake"].includes(body.command)) return new Response(null, { status: 403 });
				await mutate(() => controller.action(body.id, body.command, body.input ?? {}));
				return Response.json({ accepted: true });
			})
		};
	})
};
export { plugin_default as default };
