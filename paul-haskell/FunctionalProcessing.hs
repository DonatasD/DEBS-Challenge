module FunctionalProcessing(Source,streamFilter,streamMap,streamWindowAggregate,combineStreamWindows,joinWindowsE,joinWindowsW,
                              mergeStreams,joinStreamsE,joinStreamsW,streamFilterAcc,streamMapAcc,WindowMaker,WindowAggregator,
                              sliding,slidingTime,chop,chopTime,EventFilter,EventMap,streamMap1toN,eventMap1toN,EventMap1toN,
                              makeWindows,processWindows,JoinFilter,JoinMap) where
import FunctionalIoTtypes
import Data.Time (UTCTime,addUTCTime,diffUTCTime,NominalDiffTime)

type Source alpha = Stream alpha -- a source of data

-- Filter a Stream ...
type EventFilter alpha = alpha -> Bool                                     -- the type of the user-supplied function

streamFilter:: EventFilter alpha -> Stream alpha -> Stream alpha           -- if the value in the event meets the criteria then it can pass through
streamFilter ff []                      = []                               
streamFilter ff (e@(E t v):r) | ff v      = e      : streamFilter ff r
                              | otherwise = (T t  ): streamFilter ff r     -- always allow timestamps to pass through for use in time-based windowing
streamFilter ff (e@(V   v):r) | ff v      = e      : streamFilter ff r
                              | otherwise =          streamFilter ff r
streamFilter ff (e@(T t  ):r)             = e      : streamFilter ff r

-- Map a Stream ...
streamMap:: EventMap alpha beta -> Stream alpha -> Stream beta
streamMap fm s = map (eventMap fm) s

type EventMap alpha beta = alpha -> beta
eventMap:: EventMap alpha beta -> Event alpha -> Event beta
eventMap fm (E t v) = E t (fm v)
eventMap fm (V   v) = V   (fm v)
eventMap fm (T t  ) = T t        -- allow timestamps to pass through untouched

-- create and aggregate windows
type WindowMaker alpha = Stream alpha -> [Stream alpha]
type WindowAggregator alpha beta = [alpha] -> beta

streamWindowAggregate:: WindowMaker alpha -> WindowAggregator alpha beta -> Stream alpha -> Stream beta
streamWindowAggregate fwm fwa s = processWindows fwa (makeWindows fwm s)

makeWindows:: WindowMaker alpha -> Stream alpha -> Stream (Stream alpha)
makeWindows fwm s = map (\win->if   timedEvent (head win)    -- package the window as the value in an event
                               then E (time $ head win) win  -- if the 1st event in the window has timestamp, use that for the window
                               else V                   win) (fwm s)

processWindows:: WindowAggregator alpha beta -> Stream (Stream alpha) -> Stream beta
processWindows fwa wins = map (\win->let agg = fwa $ getVals $ value win in -- the aggregation function acts only on values in events
                                        if   timedEvent win 
                                        then E (time win) agg
                                        else V            agg) wins
            
getVals:: Stream alpha -> [alpha]
getVals s = map value $ filter dataEvent s 

splitAtValuedEvents:: Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
splitAtValuedEvents length s = splitAtValuedEvents' length [] s
   
splitAtValuedEvents':: Int -> Stream alpha -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
splitAtValuedEvents' 0      acc s                     = (True ,acc, s)
splitAtValuedEvents' length acc []                    = (False,[] ,[])
splitAtValuedEvents' length acc (h:t) | dataEvent h = splitAtValuedEvents' (length-1) (acc++[h]) t
                                      | otherwise   = splitAtValuedEvents' length     acc        t

-- Examples of WindowMaker functions
-- A sliding window of specified length : a new window is created for every event received 
sliding:: Int -> WindowMaker alpha
sliding wLength s = let (validWindow,fstWindow,rest) = splitAtValuedEvents wLength s in -- ignores events with no value
                        if   validWindow
                        then fstWindow:(sliding' fstWindow rest)
                        else []

sliding':: Stream alpha -> WindowMaker alpha
sliding' s@(_:tb) (h:t) | dataEvent h = let newWindow = tb++[h] in
                                            newWindow:(sliding' newWindow t)
                        | otherwise   = sliding' s t
sliding' _        []                  = []


--sliding2':: Stream alpha -> WindowMaker alpha
--sliding2' s@(_:tb) (h:t) = let newWindow = tb++[h] in
--                               newWindow:(sliding2' newWindow t)
--sliding2' _        []    = []

--splitAtValuedEvents2:: Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
--splitAtValuedEvents2 length s = splitAtValuedEvents2' length [] s

--splitAtValuedEvents2':: Int -> Stream alpha -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
--splitAtValuedEvents2' 0      acc s     = (True ,acc, s)
--splitAtValuedEvents2' length acc []    = (False,[] ,[])
--splitAtValuedEvents2' length acc (h:t) = splitAtValuedEvents2' (length-1) (acc++[h]) t

--A sliding window of specified length and step: a new window is created after each "step" interval
--sliding2:: Int -> Int -> WindowMaker alpha
--sliding2 wLength step s = let (validWindow,fstWindow,rest) = splitAtValuedEvents2 wLength $ filter dataEvent s in
--                             if   validWindow
--                             then fstWindow:(sliding2' fstWindow rest) --
--                             else []

slidingTime:: NominalDiffTime -> WindowMaker alpha
slidingTime tLength s@(h:t) = let endTime             = addUTCTime tLength (time h) in
                              let (revfstBuffer,rest) = timeTake endTime s in
                              let fstBuffer           = reverse revfstBuffer in
                              let validWindow         = time (head fstBuffer) >= endTime in
                                  if   validWindow
                                  then fstBuffer:(slidingTime' tLength fstBuffer rest)
                                  else []

slidingTime':: NominalDiffTime -> Stream alpha -> WindowMaker alpha
slidingTime' tLength buffer s@(h:t) = let (newEvents ,rest) = span (\e->time e==time h) t in -- find all next events with same time
                                      let startTime         = addUTCTime (-tLength) (time h) in
                                      let (newWindow,rem)   = span (\e-> time e >= startTime) (h:newEvents++buffer) in
                                      let validWindow       = (time (last newWindow) == startTime) || (not (null rem)) in
                                          if   validWindow
                                          then newWindow:(slidingTime' tLength newWindow rest)
                                          else []

timeTake:: UTCTime -> Stream alpha -> (Stream alpha,Stream alpha)
timeTake endTime s = span (\h->(time h)<=endTime) s
 
chop:: Int -> WindowMaker alpha
chop wLength s = let (validWindow,fstWindow,rest) = splitAtValuedEvents wLength s in -- remove events with no value
                     if   validWindow
                     then fstWindow:(chop wLength rest)
                     else []

chopTime:: NominalDiffTime -> WindowMaker alpha
chopTime tLength s@(h:t) = let endTime = addUTCTime tLength (time h) in
                           let (revfstBuffer,rest) = timeTake endTime s in
                           let fstBuffer           = reverse revfstBuffer in
                           let validWindow         = time (head fstBuffer) >= endTime in
                               if   validWindow
                               then fstBuffer:(chopTime tLength rest)
                               else []

-- Merge a set of streams that are of the same type. Preserve time ordering
mergeStreams:: [Stream alpha]-> Stream alpha
mergeStreams []     = []
mergeStreams (x:[]) = x
mergeStreams (x:xs) = merge' x (mergeStreams xs)

merge':: Stream alpha -> Stream alpha -> Stream alpha
merge' xs         []                                          = xs
merge' []         ys                                          = ys
merge' s1@(e1:xs) s2@(e2:ys) | timedEvent e1 && timedEvent e2 = if   time e1 < time e2 
                                                                then e1: merge' s2 xs
                                                                else e2: merge' ys s1
                             | otherwise                      = e1: merge' s2 xs  -- arbitrary ordering if 1 or 2 of the events aren't timed                                              
                                                                                  -- swap order of streams so as to interleave

-- Join 2 streams of different types by combining windows
type JoinFilter alpha beta        = alpha -> beta -> Bool
type JoinMap    alpha beta gamma  = alpha -> beta -> gamma

joinStreamsE:: Stream alpha -> WindowMaker alpha ->
               Stream beta  -> WindowMaker beta -> 
               JoinFilter alpha beta -> JoinMap alpha beta gamma -> Stream gamma
joinStreamsE s1 fwm1 s2 fwm2 fwj fwm = joinWindowsE fwj fwm $ combineStreamWindows s1 fwm1 s2 fwm2

processJoinPair:: JoinFilter alpha beta -> JoinMap alpha beta gamma -> Event (Stream alpha,Stream beta) -> Stream gamma
processJoinPair jf jm (E t (w1,w2)) = let eventPairs = cartesianProduct (filter dataEvent w1) (filter dataEvent w2) in
                                      let events     = map (\(e1,e2)-> V (value e1,value e2)) eventPairs in
                                          map (\(V (v1,v2))-> V (jm v1 v2)) 
                                          $ filter (\(V (v1,v2)) -> jf v1 v2) events

cartesianProduct:: [alpha] -> [beta] -> [(alpha,beta)]
cartesianProduct s1 s2 = [(a,b)|a<-s1,b<-s2]

joinWindowsE:: JoinFilter alpha beta -> JoinMap alpha beta gamma -> Stream (Stream alpha,Stream beta) -> Stream gamma
joinWindowsE fwj fwm s = concatMap (processJoinPair fwj fwm) s

joinStreamsW:: Stream alpha -> WindowMaker alpha ->
               Stream beta  -> WindowMaker beta  -> 
               ([alpha] -> [beta] -> gamma)      -> Stream gamma
joinStreamsW s1 fwm1 s2 fwm2 fwj =  joinWindowsW fwj $ combineStreamWindows s1 fwm1 s2 fwm2

combineStreamWindows:: Stream alpha -> WindowMaker alpha ->
                       Stream beta  -> WindowMaker beta  ->
                       Stream (Stream alpha,Stream beta)
combineStreamWindows s1 fwm1 s2 fwm2 = map (\(w1,w2) -> V (w1,w2)) $ zip (fwm1 s1) (fwm2 s2)

joinWindowsW:: ([alpha] -> [beta] -> gamma) -> Stream (Stream alpha,Stream beta) -> Stream gamma
joinWindowsW fwm s = map (\(V (w1,w2)) -> V (fwm (getVals w1) (getVals w2))) s

-- Stream Filter with accumulating parameter
streamFilterAcc:: beta -> (alpha -> beta -> beta) -> (alpha -> beta -> Bool) -> Stream alpha -> Stream alpha
streamFilterAcc acc accfn filterfn []           = []
streamFilterAcc acc accfn filterfn ((T t):rest) =         streamFilterAcc acc    accfn filterfn rest
streamFilterAcc acc accfn filterfn (e    :rest) = let  newAcc = accfn (value e) acc in 
                                                  if   filterfn (value e) acc 
                                                  then e:(streamFilterAcc newAcc accfn filterfn rest)
                                                  else   (streamFilterAcc newAcc accfn filterfn rest)

-- Stream map with accumulating parameter
streamMapAcc:: beta -> (beta -> alpha -> beta) -> (beta -> alpha -> beta) -> Stream alpha -> Stream beta
streamMapAcc acc accfn mapfn ((T t  ):rest) =                     (streamMapAcc acc           accfn mapfn rest)
streamMapAcc acc accfn mapfn ((E t v):rest) = (E t (mapfn acc v)):(streamMapAcc (accfn acc v) accfn mapfn rest)
streamMapAcc acc accfn mapfn ((V   v):rest) = (V   (mapfn acc v)):(streamMapAcc (accfn acc v) accfn mapfn rest)
streamMapAcc acc accfn mapfn []             = []

-- mapAcc example
--counter:: Stream alpha -> Stream Int
--counter s = streamMapAcc 0 (\count h-> count+1) (\count h ->count+1) s

-- Map a Stream to a set of events
streamMap1toN:: EventMap1toN alpha beta -> Stream alpha -> Stream beta
streamMap1toN mf s = concatMap (eventMap1toN mf) s

type EventMap1toN alpha beta = alpha -> [beta]
eventMap1toN:: EventMap1toN alpha beta -> Event alpha -> [Event beta]
eventMap1toN mf (E t v) = map (\nv->E t nv) $ mf v
eventMap1toN mf (V   v) = map (\nv->V   nv) $ mf v
eventMap1toN mf (T t  ) = [T t]

--- Tests ------
t1:: Int -> Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
t1 tLen sLen s = splitAtValuedEvents tLen (take sLen s)

s1:: Stream Int
s1 = [(E (addUTCTime i (read "2013-01-01 00:00:00")) 999)|i<-[0..]]

s2:: Stream Int
s2 = [T (addUTCTime i (read "2013-01-01 00:00:00")) |i<-[0..]]

s3:: Stream Int
s3 = mergeStreams [s1,s2]

s4:: Stream Int
s4 = [V i|i<-[0..]]

s5:: Stream Int
s5 = mergeStreams [s2,s4]

ex1 i = makeWindows (sliding i) s3

ex2 i = makeWindows (chop i) s3

ex3 i = makeWindows (sliding i) s4

ex4 i = makeWindows (chop i) s4

ex5 = streamFilter (\v->v>1000) s1

ex6 = streamFilter (\v->v<1000) s1

