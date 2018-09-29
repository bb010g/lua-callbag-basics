-- # Callbag 👜
--
-- A standard for Lua callbacks that enables lightweight observables and
-- iterables.
--
-- ## Specification
--
-- `(type: 'start' | 'data' | 'end', payload?: any) => void`
--
-- ### Definitions
--
-- Definitions
--
-- - *Callbag*: a function of signature
--   `(type: 'start' | 'data' | 'end', payload?: any) => void`
-- - *Greet*: if a callbag is called with `'start'` as the first argument, we
--   say "the callbag is greeted", while the code which performed the call
--   "greets the callbag"
-- - *Deliver*: if a callbag is called with `'data'` as the first argument, we
--   say "the callbag is delivered data", while the code which performed the
--   call "delivers data to the callbag"
-- - *Terminate*: if a callbag is called with `'end'` as the first argument, we
--   say "the callbag is terminated", while the code which performed the call
--   "terminates the callbag"
-- - *Source*: a callbag which is expected to deliver data
-- - *Sink*: a callbag which is expected to be delivered data
--
-- # callbag-basics
--
-- Basic callbag factories and operators to get started with using the callbag
-- spec.
--
-- Mostly ported from [@staltz](https://github.com/staltz)'s
-- JavaScript [`callbag-basics`](https://github.com/staltz/callbag-basics).

-- # API

local m = {}
local UNIQUE = {}
local unpack = unpack or table.unpack

-- ## Source factories

-- ### fromObs
-- Convert an observable (or subscribable) to a callbag listenable source.
function m.fromObs(observable)
    return function (start, sink)
        if start ~= 'start' then return end
        local dispose
        sink('start', function (t)
            if t == 'end' and dispose then
                if dispose.unsubscribe then
                    dispose.unsubscribe()
                else
                    dispose()
                end
            end
        end)
        dispose = observable.subscribe({
            next = function (x) return sink('data', x) end,
            error = function (e) return sink('end', e) end,
            complete = function () return sink('end') end,
        })
    end
end

-- ### range
-- A callbag pullable source (it only sends data when requested) that sends an
-- arithmetic progression. Goes from var (inclusive) to limit (inclusive) in
-- increments of step.
--
-- Ported from [@franciscotln](https://github.com/franciscotln)'s
-- JavaScript [`callbag-range`](https://github.com/franciscotln/callbag-range).
function m.range(var, limit, step)
    var, limit = tonumber(var), tonumber(limit)
    if step == nil then step = 1 else step = tonumber(step) end
    if not (var and limit and step) then
        error('arguments not convertible to numbers')
    end
    return function (start, sink)
        if start ~= 'start' then return end
        local inLoop = false
        local gotData = false
        local completed = false
        local function loop()
            inLoop = true
            while gotData and not completed do
                gotData = false
                if (step > 0 and var > limit) or (step <= 0 and var < limit)
                then
                    sink('end')
                    completed = true
                else
                    sink('data', var)
                    var = var + step
                end
            end
            inLoop = false
        end
        sink('start', function (t)
            if completed then return end
            if t == 'data' then
                gotData = true
                if not inLoop then return loop() end
            elseif t == 'end' then
                completed = true
            end
        end)
    end
end

-- ### fromArray
-- Convert an array-like object (integer indices increasing from 1) to a
-- callbag pullable source (it only sends data when requested).
function m.fromArray(arr)
    return function (start, sink)
        if start ~= 'start' then return end
        local inLoop = false
        local gotData = false
        local completed = false
        local index, res = 1, nil
        local function loop()
            inLoop = true
            while gotData and not completed do
                gotData = false
                res = arr[index]
                if res == nil then
                    sink('end')
                    completed = true
                else
                    sink('data', res)
                    index = index + 1
                end
            end
            inLoop = false
        end
        sink('start', function (t)
            if completed then return end
            if t == 'data' then
                gotData = true
                if not inLoop then loop() end
            elseif t == 'end' then
                completed = true
            end
        end)
    end
end

-- ### fromIter
-- Convert a Lua iterator to a callbag pullable source (it only sends data when
-- requested).
function m.fromIter(f, s, var)
    return function (start, sink)
        if start ~= 'start' then return end
        local inLoop = false
        local gotData = false
        local completed = false
        local res
        local function loop()
            inLoop = true
            while gotData and not completed do
                gotData = false
                res = {f(s, var)}
                var = res[1]
                if var == nil then
                    sink('end')
                    break
                else
                    sink('data', res)
                end
            end
            inLoop = false
        end
        sink('start', function (t)
            if completed then return end
            if t == 'data' then
                gotData = true
                if not inLoop and var ~= nil then loop() end
            elseif t == 'end' then
                completed = true
            end
        end)
    end
end

-- ## Sink factories

-- ### forEach
-- Callbag sink that consumes both pullable and listenable sources. When called
-- on a pullable source, it will iterate through its data. When called on a
-- listenable source, it will observe its data.
function m.forEach(operation) return function (source)
    local talkback
    source('start', function (t, d)
        if t == 'start' then talkback = d
        elseif t == 'data' then operation(d) end
        if t == 'start' or t == 'data' then talkback('data') end
    end)
end end

-- ### intoIter
-- Callbag sink that converts a synchronous pullable source to a Lua iterator
-- consumer.
function m.toIter(source)
    return coroutine.wrap(function ()
        local talkback
        local var
        source('start', function (t, d)
            if t == 'start' then talkback = d
            elseif t == 'data' then var = d
            elseif t == 'end' then talkback = nil end
        end)
        while talkback do
            talkback('data')
            if var ~= nil then
                coroutine.yield(var)
                var = nil
            else
                if talkback then talkback('end') end
                return
            end
        end
    end)
end

-- ## Transformation operators

-- ### map
-- Callbag operator that applies a transformation on data passing through it.
-- Works on either pullable or listenable sources.
function m.map(f) return function (source)
    return function (start, sink)
        if start ~= 'start' then return end
        source('start', function (t, d)
            if t == 'data' then d = f(d) end
            sink(t, d)
        end)
    end
end end

-- ### scan
-- Callbag operator that combines consecutive values from the same source. It's
-- essentially like JavaScript's array `.reduce`, but delivers a new accumulated
-- value for each value from the callbag source. Works on either pullable or
-- listenable sources.
function m.scan(...)
    local reducer, seed = ...
    local hasAcc = select('#', ...) == 2
    return function (source)
        return function (start, sink)
            if start ~= 'start' then return end
            local acc = seed
            source('start', function (t, d)
                if t == 'data' then
                    if hasAcc then
                        acc = reducer(acc, d)
                    else
                        hasAcc = true
                        acc = d
                    end
                    sink(t, acc)
                else sink(t, d) end
            end)
        end
    end
end

-- ### flatten
-- Callbag operator that flattens a higher-order callbag source. Like RxJS
-- "switch" or xstream "flatten". Use it with map to get behavior equivalent to
-- "switchMap". Works on either pullable or listenable sources.
function m.flatten(source)
    return function (start, sink)
        if start ~= 'start' then return end
        local outerEnded = false
        local outerTalkback, innerTalkback
        local function talkback(t, d)
            if t == 'data' then (innerTalkback or outerTalkback)(t, d)
            elseif t == 'end' then
                if innerTalkback then innerTalkback(t) end
                outerTalkback(t)
            end
        end
        source('start', function (T, D)
            if T == 'start' then
                outerTalkback = D
                sink(T, talkback)
            elseif T == 'data' then
                local innerSource = D
                if innerTalkback then innerTalkback('end') end
                innerSource('start', function (t, d)
                    if t == 'start' then
                        innerTalkback = d
                        innerTalkback('data')
                    elseif t == 'data' then sink(t, d)
                    elseif t == 'end' then
                        if d then
                            outerTalkback(t)
                            sink(t, d)
                        else
                            if outerEnded then sink(t)
                            else
                                innerTalkback = nil
                                outerTalkback('data')
                            end
                        end
                    end
                end)
            elseif T == 'end' then
                if D then
                    if innerTalkback then innerTalkback(T) end
                    sink(T, D)
                else
                    if not innerTalkback then sink(T)
                    else outerEnded = true end
                end
            end
        end)
    end
end


-- ## Filtering operators

-- ### take
-- Callbag operator that limits the amount of data sent by a source. Works on
-- either pullable and listenable sources.
function m.take(max) return function (source)
    return function (start, sink)
        if start ~= 'start' then return end
        local taken = 0
        local sourceTalkback
        local function talkback(t, d)
            if taken < max then sourceTalkback(t, d) end
        end
        source('start', function (t, d)
            if t == 'start' then
                sourceTalkback = d
                sink(t, talkback)
            elseif t == 'data' then
                if taken < max then
                    taken = taken + 1
                    sink(t, d)
                    if taken == max then
                        sink('end')
                        sourceTalkback('end')
                    end
                end
            else
                sink(t, d)
            end
        end)
    end
end end

-- ### skip
-- Callbag operator that skips the first N data points of a source. Works on
-- either pullable and listenable sources.
function m.skip(max) return function (source)
    return function (start, sink)
        if start ~= 'start' then return end
        local skipped = 0
        local talkback
        source('start', function (t, d)
            if t == 'start' then
                talkback = d
                sink(t, d)
            elseif t == 'data' then
                if skipped < max then
                    skipped = skipped + 1
                    talkback('data')
                else sink(t, d) end
            else
                sink(t, d)
            end
        end)
    end
end end

-- ### filter
-- Callbag operator that conditionally lets data pass through. Works on either
-- pullable or listenable sources.
function m.filter(condition) return function (source)
    return function(start, sink)
        if start ~= 'start' then return end
        local talkback
        source('start', function (t, d)
            if t == 'start' then
                talkback = d
                sink(t, d)
            elseif t == 'data' then
                if condition(d) then sink(t, d)
                else talkback('data') end
            else sink(t, d) end
        end)
    end
end end

-- ## Combination operators

-- ### merge
-- Callbag factory that merges data from multiple callbag sources. Works well
-- with listenable sources, and while it may work for some pullable sources, it
-- is only designed for listenable sources.

-- ### concat
-- Callbag factory that concatenates the data from multiple (2 or more) callbag
-- sources. It starts each source at a time: waits for the previous source to
-- end before starting the next source. Works with both pullable and listenable
-- sources.
function m.concat(...)
    local n = select('#', ...)
    local sources = {...}
    return function (start, sink)
        if start ~= 'start' then return end
        if n == 0 then
            sink('start', function () end)
            sink('end')
            return
        end
        local i = 1
        local sourceTalkback
        local lastPull = UNIQUE
        local function talkback(t, d)
            if t == 'data' then lastPull = d end
            sourceTalkback(t, d)
        end
        local function next()
            if i > n then
                sink('end')
                return
            end
            sources[i]('start', function (t, d)
                if t == 'start' then
                    sourceTalkback = d
                    if i == 1 then
                        sink(t, talkback)
                    elseif lastPull ~= UNIQUE then
                        sourceTalkback('data', lastPull)
                    end
                elseif t == 'end' then
                    if d then
                        sink(t, d)
                    else
                        i = i + 1
                        return next()
                    end
                else
                    sink(t, d)
                end
            end)
        end
        return next()
    end
end

-- ### combine
-- Callbag factory that combines the latest data points from multiple (2 or
-- more) callbag sources. It delivers those latest values as an array. Works
-- with both pullable and listenable sources.

-- ## Utilities

-- ### share
-- Callbag operator that broadcasts a single source to multiple sinks. Does
-- reference counting on sinks and starts the source when the first sink gets
-- connected, similar to RxJS
-- [`.share()`](https://www.learnrxjs.io/operators/multicasting/share.html).
-- Works on either pullable or listenable sources.
function m.share(source)
    local sinks = {}
    local sourceTalkback

    local function shared(start, sink)
        if start ~= 'start' then return end
        table.insert(sinks, sink)

        local function talkback(t, d)
            if t == 'end' then
                local i, found = 1, false
                while not found do
                    local v = sinks[i]
                    if v == nil then break end
                    if v == sink then found = true end
                    i = i + 1
                end
                if found then table.remove(sinks, i) end
                if #sinks == 0 then sourceTalkback(t) end
            else
                sourceTalkback(t, d)
            end
        end

        if #sinks == 1 then
            source('start', function (t, d)
                if t == 'start' then
                    sourceTalkback = d
                    sink(t, talkback)
                else
                    local sinksCopy = {}
                    for i, v in ipairs(sinks) do sinksCopy[i] = v end
                    for _, s in ipairs(sinksCopy) do s(t, d) end
                end
                if t == 'end' then sinks = {} end
            end)
        else
            sink('start', talkback)
        end
    end
    return shared
end

-- ### pipe
-- Utility function for plugging callbags together in chain. This utility
-- actually doesn't rely on Callbag specifics, and is basically the same as
-- Ramda's `pipe` or lodash's `flow`. Anyway, this exists just to play nicely
-- with the ecosystem, and to facilitate the import of the function.
function m.pipe(...)
    local res = {(...)}
    for i = 2, select('#', ...) do
        res = {select(i, ...)(unpack(res))}
    end
    return unpack(res)
end

-- ### pipeValues
-- Like pipe, but the first argument is an array-style table of initial values
-- to pass.
function m.pipeValues(...)
    local res = ...
    for i = 2, select('#', ...) do
        res = {select(i, ...)(unpack(res))}
    end
    return unpack(res)
end

return m
