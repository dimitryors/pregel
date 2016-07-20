<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>
<!--a href="https://travis-ci.org/tarantool/pregel">
	<img src="https://travis-ci.org/tarantool/pregel.png?branch=master" align="right">
</a-->

# Large-scale graph processing (powered by Tarantool)

Based on [Pregel whitepaper](http://kowshik.github.io/JPregel/pregel_paper.pdf)
written by Kowshik Prakasam and Manasa Chandrasheka
(see the [original site](http://kowshik.github.io/JPregel/index.html)).

As the 'abstract' says:

> Many practical computing problems concern large graphs. Standard examples
> include the Web graph and various social networks. The scale of these
> graphs - in some cases billions of vertices, trillions of edges — poses
> challenges to their efficient processing. In this paper we present a
> computational model suitable for this task. Programs are expressed as a
> sequence of iterations, in each of which a vertex can receive messages sent
> in the previous iteration, send messages to other vertices, and modify its
> own state and that of its outgoing edges or mutate graph topology. This
> vertex-centric approach is flexible enough to express a broad set of
> algorithms. The model has been designed for efficient, scalable and
> fault-tolerant implementation on clusters of thousands of commodity
> computers, and its implied synchronicity makes reasoning about programs
> easier. Distribution-related details are hidden behind an abstract API.
> The result is a framework for processing large graphs that is expressive
> and easy to program.

The API was inspired by [Apache Giraph](http://giraph.apache.org/).

## Table of contents

* [Configuration reference](#configuration-reference)
  * [Common configuration options (for master and worker)](#common-configuration-options-for-master-and-worker)
  * [Preloading configuration options](#preloading-configuration-options)
    * [API for loader](#api-for-loader)
  * [Worker configuration options](#worker-configuration-options)
* [Developer's guide](#developers-guide)
  * [Master node](#master-node)
  * [Worker node](#worker-node)
  * [Vertex](#vertex)
    * [API](#api)
* [Simple Avro file reader and utils library](#simple-avro-file-reader-and-utils-library)

## Configuration reference

### Common configuration options (for master and worker)

* `workers` -- required -- a list of all
  [URIs](https://tarantool.org/doc/book/configuration/index.html#uri)
	 with workers.

  Type: `table` of `string`s

	 Example:

	``` lua
	local workers = {
		'myhost1:myport1',
		'myhost1:myport2',
		'myhost2:myport3',
	}
	-- or, for simple generation:
	local fun = require('fun')
	local function generate_worker_uris(cnt)
		cnt = cnt or 8
		return fun.range(cnt):map(function(k)
			return 'myhost1:' .. tostring(3301 + k)
		end):chain(fun.range(cnt):map(function(k)
			return 'myhost2:' .. tostring(3301 + k)
		end)):chain(fun.range(cnt):map(function(k)
			return 'myhost3:' .. tostring(3301 + k)
		end)):totable()
	end
	local workers = generate_worker_uris(cnt)
	```

* `pool_size` -- the size of the pool for outgoing messages.

   Type: `number`

* `obtain_name` -- required -- a callback for obtaining the vertex name from data.


   Syntax: `function (value) -> string`

  	Example:
	``` lua
	local function obtain_name(value)
		return value.name
	end
	-- or
	local function obtain_name(value)
		return ('%s:%s'):format(value.part1, value.part2)
	end
	```

### Preloading configuration options

* `worker_preload` -- a function to preload data to nodes (from all workers);
	 excludes `master_preload`

	 Syntax: (`function (worker, opts) -> (function(self, idx, cnt) -> nil)`)

* `master_preload` -- a function to preload data to nodes (from the master only);
	 excludes `worker_preload`

	 Syntax: (`function (master, opts) -> (function(self) -> nil)`)

* `preload_args` -- arguments that must be passed to the loader

Let's start with examples of creating a worker/master loader:

``` lua
local ploader = require('pregel.loader')

local function worker_loader(worker, opts)
	local function loader(self, worker_idx, workers_count)
	-- loading process
	end
	return ploader.new(worker, loader)
end

local function master_loader(master, opts)
	local function loader(self)
	-- loading process
	end
	return ploader.new(master, loader)
end
```

Later on we'll provide `worker_loader`/`master_loader` as parameters in the
options table.

#### API for loader

* `loader:store_vertex(vertex)` -- store an arbitrary vertex object
* `loader:store_edge(src, dest, value)` -- store the direct edge between 
	vertices with the names `dest` and `value`
* `loader:store_edges_batch(edge_list)` -- store a number of edges
* `loader:store_vertex_edges(vertex, edge_list)` -- a combination of `store_vertex`
	and `store_edges_batch`
* `loader:flush()` -- send out all cached vertices

### Worker configuration options

* `worker_context` -- set the worker context (a common object for all vertices
	located on the current instance of a worker; can be accessed/modified by a vertex).
* `master` -- [URI](https://tarantool.org/doc/book/configuration/index.html#uri)
	of the master node
* `combiner` -- combine messages that need to be sent to a vertex
* `compute` -- compute function

## Developer's guide

In the `example` folder, we provide a simple example that finds the biggest value among all vertices using communication:
1 First, every vertex sends a message with its current value to its neighbors.
1 If a vertex receives a value which is greater than the value it had or received before,
  then it informs all the neighbors.
1 The communication is over when no messages are sent and all vertices
  have got the biggest value.

For more usages of Pregel data model, you can read [this paper](http://kowshik.github.io/JPregel/pregel_paper.pdf)
and the [original site](http://kowshik.github.io/JPregel/).

For example:
* Shortest Path:
	- http://kowshik.github.io/JPregel/developers.html#shortestpaths
	- https://cwiki.apache.org/confluence/display/GIRAPH/Shortest+Paths+Example
* PageRank Algorithm
	- http://kowshik.github.io/JPregel/developers.html#pagerank
	- http://giraph.apache.org/pagerank.html
* etc.

In future, we're planning to provide more examples/algorithms.

Also, it's recommended to use `tarantool-pregel` in conjuction with [Torch](http://torch.ch/).

> Torch is a scientific computing framework with wide support for machine
> learning algorithms that puts GPUs first. It is easy to use and efficient,
> thanks to an easy and fast scripting language, LuaJIT.

But in this case, please remember not to use parallelization (as it'll break Tarantool evloop) or GPU
(since it isn't integrated with our fibers, so this will stop Tarantool).

### Master node

Master node is something that orchestrates everything. It tells the workers what to do
, and it has access to all their results in the end.

* `master:wait_up()` -- wait until all the workers are up and running.
* `master:start()` -- start the task (compute everything)
* `master:preload()` -- preload all data from the master
* `master:preload_on_workers()` -- preload all data from the workers
* `master:add_aggregator(name, options)` -- add an aggregator

	Possible options are:
	* `default` -- a default value for an aggregator. Can be anything.
	* `merge` -- add a new value to an aggregator. Must be commutative and associative.

	  Example:
		```
		local function merge(old, new)
			return old + new
		end
		```

* `master:save_snapshot()` -- tell the workers to save a snapshot.

Typical master initialization is like that:

```
local pmaster = require('pregel.master')
local master = pmaster.new('test', config)
master:add_aggregator('custom', <options>)
master:wait_up()
master:preload_on_workers()
<...> -- for example, you can add vertices manually, if you need to.
master:save_snapshot()
```

### Worker node

Worker node is the basic computing unit. It can't be parallelized, for now.  
A worker maintains its state and its part of data (vertices and ingoing/outgoing messages).

* `master:add_aggregator(name, options)` -- add an aggregator.

	Possible options are:
	* `default` -- a default value for an aggregator. Can be anything.
	* `merge` -- add a new value to an aggregator. Must be commutative and associative.

	  Example:
		```
		local function merge(old, new)
			return old + new
		end
		```

### Vertex

Vertex is the last and main part of this process. The compute function is applied
to each vertex on each worker node. A vertex has the following fields:

* Vertex name
* "Is halted" flag
* Vertex value
* Outgoing edges (pairs of \<destination name, vertex 'weight' (its value)\>)
* All incoming messages

#### API

**Base API**

* `vertex:vote_halt([is_halted = true])` -- set the vertex status to be halted
* `vertex:pairs_edges()` -- iterate through all edges

	Example:
	```
	for _, neighbor, value in vertex:pairs_edges() do
		-- process vertex
	end
	```

* `vertex:get_value()` -- get the vertex value
* `vertex:set_value(value)` -- set the vertex value
* `vertex:get_name()` -- get the vertex name
* `vertex:get_superstep()` -- get the superstep number (1 to ...)

**Messaging API**

> Vertices communicate directly with one another by sending messages, each of
> which consists of a message value and the name of the destination vertex.
> A vertex can send any number of messages in a superstep. All messages sent to
> vertex V in superstep S are available, via an iterator, when V’s `Compute()`
> method is called in superstep S + 1. There is no guaranteed order of messages
> in the iterator, but it is guaranteed that messages will be delivered and that
> they will not be duplicated. A common usage pattern is for a vertex V to
> iterate over its outgoing edges, sending a message to the destination vertex
> of each edge.
>
> However, `dest_vertex` need not be a neighbor of V. A vertex could learn the
> identifier of a non-neighbor from a message received earlier, or vertex
> identifiers could be known implicitly. For example, the graph could be a
> clique, with well-known vertex identifiers V1 through Vn, in which case there
> may be no need to even keep explicit edges in the graph. When the destination
> vertex of any message does not exist, we execute user-defined handlers. A
> handler could, for example, create the missing vertex or remove the dangling
> edge from its source vertex.

* `vertex:pairs_messages()` -- iterate through all incoming messages.

	Example:
	```
	for _, message in vertex:pairs_messages() do
		-- process all incoming messages
	end
	```
* `vertex:send_message(receiver_id, msg)` -- send a message to the vertex with ID
	`receiver_id`

Note: Under the hood, every contact with another node involves sending a message,
so you can send an arbitrary message to other nodes using the so-called
`mpool`. For more reference on this, please, see the source code
([here](https://github.com/tarantool/pregel/blob/master/pregel/mpool.lua)
and [here](https://github.com/tarantool/pregel/blob/master/pregel/master.lua#L56)).

**Aggregation API**

> Pregel aggregators are a mechanism for global communication, monitoring, and
> data. Each vertex can provide a value to an aggregator in superstep S, the
> system combines those values using a reduction operator, and the resulting
> value is made available to all vertices in superstep S + 1.

* `vertex:get_aggragation(name)` -- get a value from an aggregator
* `vertex:set_aggregation(name, value)` -- set an aggregator value

**Topology mutation API**

> Some graph algorithms need to change the graph’s topology. A clustering
> algorithm, for example, might replace each cluster with a single vertex,
> and a minimum spanning tree algorithm might remove all but the tree edges.
> Just as a user’s Compute() function can send messages, it can also issue
> requests to add or remove vertices or edges.

* `vertex:add_vertex(value)` -- add a vertex
* `vertex:add_edge([src = vertex:get_name(), ]dest, value)` -- add an edge
* `vertex:delete_vertex([src][, vertices = true])` -- delete a vertex
* `vertex:delete_edge([src = vertex:get_id(), ]dest)` -- delete an edge

If you change the properties (add/delete vertex/edge) of a currently running vertex,
the changes will be applied immediately after compute.

## Simple Avro file reader and utils library

TBD
