(ns backtype.storm.daemon.rest
  (:import [org.restlet.data Protocol ])
  (:import [org.apache.thrift7.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift7.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift7 TException])
  (:import [org.apache.thrift7.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [backtype.storm.generated RESTful RESTful$RESTfulProcessor RESTful$RESTfulServer
            DRPCRequest DRPCExecutionException DistributedRPCInvocations 
            DistributedRPCInvocations$Iface DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [backtype.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:use [backtype.storm bootstrap config log])
  (:gen-class))

(bootstrap)

(def TIMEOUT-CHECK-SECS 5)

(defn acquire-queue [queue-atom function]
  (swap! queue-atom
         (fn [amap]
           (if-not (amap function)
             (assoc amap function (ConcurrentLinkedQueue.))
             amap)
           ))
  (@queue-atom function))

(defn service-handler []
  (let [conf (read-storm-config)
        ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                  (swap! id->result dissoc id)
                  (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        clear-thread (async-loop
                      (fn []
                        (doseq [[id start] @id->start]
                          (when (> (time-delta start) (conf REST-REQUEST-TIMEOUT-SECS))
                            (when-let [sem (@id->sem id)]
                              (swap! id->result assoc id (DRPCExecutionException. "Request timed out"))
                              (.release sem))
                            (cleanup id)
                            ))
                        TIMEOUT-CHECK-SECS
                        ))
        ]
    (reify RESTful$RESTfulProcessor
      (^String execute [this ^String uri]
        (log-debug "Received REST request for " uri " at " (System/currentTimeMillis))
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. uri id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues "REST")
              ]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (.add queue req)
          (log-debug "Waiting for REST result for " uri " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired REST result for " uri " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning REST result as " result " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              result
             ))))
      DistributedRPCInvocations$Iface
      (^void result [this ^String id ^String result]
        (let [^Semaphore sem (@id->sem id)]
          (log-debug "Receive Result " result " for " id " at " (System/currentTimeMillis))
          (when sem
            (swap! id->result assoc id result)
            (.release sem)
            )))
      (^void failRequest [this ^String id]
        (let [^Semaphore sem (@id->sem id)]
          (when sem
            (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
            (.release sem)
            )))
      ;; function name defaults to be "REST"
      (^DRPCRequest fetchRequest [this ^String func]
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for REST at " (System/currentTimeMillis))
              ret)
            (DRPCRequest. "" ""))
          ))
      Shutdownable
      (shutdown [this]
        (.interrupt clear-thread))
      )))

(defn launch-server!
  ([]
    (let [conf (read-storm-config)
          worker-threads (int (conf REST-WORKER-THREADS))
          queue-size (int (conf REST-QUEUE-SIZE))
          service-handler (service-handler)
          
          handler-server (.createServer
                           (RESTful$RESTfulServer. service-handler)
                           (Protocol/HTTP)
                           (int (conf REST-PORT))
                           )
          
          invoke-server (THsHaServer. (-> (TNonblockingServerSocket. (int (conf REST-INVOCATIONS-PORT)))
                                        (THsHaServer$Args.)
                                        (.workerThreads 64)
                                        (.protocolFactory (TBinaryProtocol$Factory.))
                                        (.processor (DistributedRPCInvocations$Processor. service-handler))
                                        ))]
      (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop handler-server)
                                                        (.stop invoke-server))))
      (log-message "Starting REST servers...")
      (future (.serve invoke-server))
      (.start handler-server))))

(defn -main []
  (launch-server!))
      