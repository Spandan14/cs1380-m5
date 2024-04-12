const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  const util = global.distribution.util;

  let mapCompletions = 0;
  let mappedKeys = new Set();

  let reduceCompletions = 0;
  let reducedKeys = new Set();

  const MAP_COMPLETION_MESSAGE = 'MAP_COMPLETE';
  const REDUCE_COMPLETION_MESSAGE = 'REDUCE_COMPLETE';

  let looper = (arr, func, i, continuation) => {
    if (i >= arr.length) {
      continuation();
      return;
    }

    func(arr[i], (e, v) => {
      if (e) {
        console.log(`Error in looper for func ${func}: ${e}`);
      }

      looper(arr, func, i + 1, continuation);
    });
  };

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      callback = callback || function() {};

      // setup compaction
      if (!configuration.compact) {
        configuration.compact = (results) => {
          let compacted = {};
          for (let i = 0; i < results.length; i++) {
            for (let [key, value] of Object.entries(results[i])) {
              if (!compacted[key]) {
                compacted[key] = [];
              }

              compacted[key].push(value);
            }
          }

          return compacted;
        };
      }

      // setup completions
      distribution.local.groups.get(context.gid, (e, v) => {
        if (e) {
          console.log('Error getting group: ', e);
        }

        let group = v;

        // get all the values
        let values = [];
        let valueGetter = (key, callback) => {
          distribution[context.gid].store.get(key, (e, v) => {
            if (e) {
              console.log('Error getting value: ', e);
            }

            values.push(v);
            callback(e, v);
          });
        };
        looper(configuration.keys, valueGetter, 0, () => {
          // id gen
          const date = Date.now().toString(36);
          const rand = Math.random().toString(36).substring(2, 9);
          let mapReduceID = `mr-${date}-${rand}`;

          // setup notification service
          let mrNotificationService = {};
          mrNotificationService.notify = (msg, keys) => {
            console.log(`NOTIFICATION RECEIVED: `, msg);
            console.log(`KEYS: `, keys);

            if (msg === MAP_COMPLETION_MESSAGE) {
              mapCompletions++;
              for (let i = 0; i < keys.length; i++) {
                mappedKeys.add(keys[i]);
              }
              if (mapCompletions === Object.keys(group).length) {
                // all maps are done
                // get all the data
                let mapResults = [];
                let reducerKeys = [];
                let resultGetter = (key, callback) => {
                  let hash = util.id.getID(key);
                  let hashedKey = `mr-mapResult-${mapReduceID}-${hash}`;
                  distribution[context.gid].store.get(hashedKey, (e, v) => {
                    if (e) {
                      console.log('Error getting result: ', e);
                    }

                    mapResults.push(v);
                    // i think 34 works here
                    reducerKeys.push(key);
                    callback(e, v);
                  });
                };

                looper(Array.from(mappedKeys), resultGetter, 0, () => {
                  // all results are in

                  // setup reduce system
                  let thisNode = global.nodeConfig;
                  let reduceRoutine = (keys, values, callback) => {
                    console.log('REDUCTION KEYS: ', keys);
                    // compute reduce on payload
                    results = [];

                    for (let i = 0; i < keys.length; i++) {
                      let reduced = configuration.reduce(keys[i], values[i]);

                      for (let [key, value] of Object.entries(reduced)) {
                        results.push({key: key, value: value});
                      }
                    };

                    // TODO: check if we need to compact here

                    let finalKeys = [];
                    let distributor = (obj, callback) => {
                      let hash = util.id.getID(obj.key);
                      finalKeys.push(obj.key);
                      let hashedKey = `mr-reduceResult-${mapReduceID}-${hash}`;
                      distribution[context.gid].store.append(obj.value,
                          hashedKey, callback);
                    };
                    looper(results, distributor, 0, () => {
                      // setup notification
                      let remote = {service: mrNotificationServiceID,
                        method: 'notify', node: thisNode};

                      let payload = [REDUCE_COMPLETION_MESSAGE, finalKeys];

                      // send notification
                      distribution.local.comm.send(payload, remote, (e, v) => {
                        if (e) {
                          console.log('Error sending message to leader: ', e);
                        }
                        callback(e, v);
                      });
                    });
                  };

                  let reduceRPC = util.wire.createRPC(util.wire.
                      toAsync(reduceRoutine));

                  let reduceFuncString = `
                    let reduceFunc = ${reduceRPC.toString()};
                    reduceFunc(arguments[0], arguments[1], arguments[2]);
                  `;

                  let reduceFunction = Function(reduceFuncString);

                  const mrReduceServiceID = `mr-reduce-${mapReduceID}`;

                  // now register services remotely on other nodes in the group
                  remote = {service: 'routes', method: 'put'};
                  payload = [{reduce: reduceFunction}, mrReduceServiceID];


                  distribution[context.gid].comm.send(payload,
                      remote, (e, v) => {
                        if (e) {
                          console.log('Error setting up reduce system: ', e);
                        }

                        // distribute to reduce
                        let keySets = [];
                        let valueSets = [];
                        let setSize = Math.ceil(mapResults.length /
                          Object.keys(group).length);

                        for (let i = 0; i < mapResults.length; i += setSize) {
                          keySets.push(Array.from(reducerKeys).
                              slice(i, i + setSize));
                          valueSets.push(mapResults.slice(i, i + setSize));
                        }

                        let groupList = Object.values(group);

                        let reduceSender = (i, callback) => {
                          let remote = {service: mrReduceServiceID,
                            method: 'reduce',
                            node: groupList[i]};
                          let payload = [keySets[i], valueSets[i]];

                          distribution.local.comm.send(payload, remote,
                              (e, v) => {
                                if (e) {
                                  console.log('Error sending reduce request: ',
                                      e);
                                }

                                callback(e, v);
                              });
                        };

                        let indices = Array.from({length: groupList.length},
                            (v, i) => i);
                        looper(indices, reduceSender, 0, () => {});
                      });
                });
              }
            } else if (msg === REDUCE_COMPLETION_MESSAGE) {
              reduceCompletions++;
              for (let i = 0; i < keys.length; i++) {
                reducedKeys.add(keys[i]);
              }
              if (reduceCompletions === Object.keys(group).length) {
                // all reduces are done
                // get all the data
                let reduceResults = [];
                let resultGetter = (key, cb) => {
                  console.log('GETTING KEY RESULT: ', key);

                  let hash = util.id.getID(key);
                  let hashedKey = `mr-reduceResult-${mapReduceID}-${hash}`;
                  distribution[context.gid].store.get(hashedKey, (e, v) => {
                    if (e) {
                      console.log('Error getting result: ', e);
                    }

                    reduceResults.push(v);
                    cb(e, v);
                  });
                };

                looper(Array.from(reducedKeys), resultGetter, 0, () => {
                  console.log('IN FINAL COLLECTION');
                  // all results are in
                  // callback with final results
                  let output = [];
                  for (let i = 0; i < reduceResults.length; i++) {
                    let key = Array.from(reducedKeys)[i];
                    let finalKey = key;

                    if (!key) {
                      continue;
                    }

                    let value = reduceResults[i];
                    let obj = {};
                    obj[finalKey] = value[0];
                    output.push(obj);
                  }

                  console.log('OUTPUT: ', output);

                  // clean up all the files
                  let mapCleaner = (key, cb) => {
                    let hash = util.id.getID(key);
                    let hashedKey = `mr-mapResult-${mapReduceID}-${hash}`;
                    distribution[context.gid].store.del(hashedKey, cb);
                  };

                  let reduceCleaner = (key, cb) => {
                    let hash = util.id.getID(key);
                    let hashedKey = `mr-reduceResult-${mapReduceID}-${hash}`;
                    distribution[context.gid].store.del(hashedKey, cb);
                  };

                  looper(Array.from(mappedKeys), mapCleaner, 0, () => {
                    looper(Array.from(reducedKeys), reduceCleaner, 0, () => {
                      callback(null, output);
                    });
                  });
                });
              }
            }
          };

          const mrNotificationServiceID = `mr-host-notify-${mapReduceID}`;

          // register notification service
          distribution.local.routes.put(mrNotificationService,
              mrNotificationServiceID, (e, v) => {
                if (e) {
                  console.log('Error registering notification service: ', e);
                  // callback(e, v);
                }

                // setup MAP service for remote nodes
                let thisNode = global.nodeConfig;
                let mapRoutine = (keys, values, callback) => {
                  // compute map on payload
                  results = [];
                  // TODO: may need to make this async-friendly
                  for (let i = 0; i < keys.length; i++) {
                    let mapped = configuration.map(keys[i], values[i]);

                    // mapped can give multiple outputs
                    if (!Array.isArray(mapped)) {
                      mapped = [mapped];
                    }

                    for (let object of mapped) {
                      results.push(object);
                    }
                  }

                  // compact results of map
                  console.log('PRE COMPACTION, ', results);
                  compactedResults = configuration.compact(results);
                  console.log('POST COMPACTION, ', compactedResults);
                  results = [];
                  for (let [key, value] of Object.entries(compactedResults)) {
                    results.push({key: key, value: value});
                  }

                  let finalKeys = [];

                  // shuffle results of map in a distributed way
                  // grouping happens at the same time with append
                  let shuffler = (obj, callback) => {
                    finalKeys.push(obj.key);
                    let hash = util.id.getID(obj.key);
                    let fileKey = `mr-mapResult-${mapReduceID}-${hash}`;
                    distribution[context.gid].store.append(obj.value,
                        fileKey, callback);
                  };
                  looper(results, shuffler, 0, () => {
                    // setup notification
                    let remote = {service: mrNotificationServiceID,
                      method: 'notify', node: thisNode};

                    let payload = [MAP_COMPLETION_MESSAGE, finalKeys];

                    // send notification
                    distribution.local.comm.send(payload, remote, (e, v) => {
                      if (e) {
                        console.log('Error sending message to leader: ', e);
                      }
                      callback(e, v);
                    });
                  });
                };

                // must RPC this - host doesn't know who the orchestrator is
                let mapRPC = util.wire.createRPC(util.wire.toAsync(mapRoutine));

                let mapFuncString = `
                  let mapFunc = ${mapRPC.toString()};
                  mapFunc(arguments[0], arguments[1], arguments[2]);
                `;

                let mapFunction = Function(mapFuncString);

                const mrMapServiceID = `mr-map-${mapReduceID}`;

                // now register services remotely on other nodes in the group
                let remote = {service: 'routes', method: 'put'};
                let payload = [{map: mapFunction}, mrMapServiceID];

                distribution[context.gid].comm.send(payload, remote, (e, v) => {
                  if (e) {
                    console.log('Error setting up map system: ', e);
                  // callback(e, v);
                  }

                  // distribute keys to every node to map
                  let keySets = [];
                  let valueSets = [];
                  let setSize = Math.ceil(configuration.keys.length /
                    Object.keys(group).length);
                  for (let i = 0; i < configuration.keys.length;
                    i += setSize) {
                    keySets.push(configuration.keys.slice(i,
                        i + setSize));
                    valueSets.push(values.slice(i, i + setSize));
                  };

                  let groupList = Object.values(group);

                  let mapSender = (i, callback) => {
                    let remote = {service: mrMapServiceID, method: 'map',
                      node: groupList[i]};
                    let payload = [keySets[i], valueSets[i]];

                    distribution.local.comm.send(payload, remote,
                        (e, v) => {
                          if (e) {
                            console.log('Error sending map request: ', e);
                          }

                          callback(e, v);
                        });
                  };
                  let indices = Array.from({length: groupList.length},
                      (v, i) => i);
                  looper(indices, mapSender, 0, () => {});
                });
              });
        });
      });
    },
  };
};

module.exports = mr;
