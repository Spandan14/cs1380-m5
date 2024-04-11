const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  const util = global.distribution.util;

  let mapCompletions = new Set();
  let reduceCompletions = new Set();

  const MAP_COMPLETION_MESSAGE = 'MAP_COMPLETE';
  const REDUCE_COMPLETION_MESSAGE = 'REDUCE_COMPLETE';

  let looper = (arr, func, i, continuation) => {
    if (i >= arr.length) {
      continuation();
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
            let key = results[i].key;
            let value = results[i].value;

            if (!compacted[key]) {
              compacted[key] = [];
            }

            compacted[key].push(value);
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
          mrNotificationService.notify = (msg, nid) => {
            console.log(`NOTIFICATION RECEIVED FROM ${nid}: `, msg);

            if (msg === MAP_COMPLETION_MESSAGE) {
              mapCompletions.add(nid);
              console.log("MAP COMPLETIONS: ", mapCompletions.size);
              if (mapCompletions.size === Object.keys(group).length) {
              // all maps are done
                console.log('WHAT THE FUCK ALL MAPS ARE DONE SHEESH!');
                callback(null, null);
              }
            } else if (msg === REDUCE_COMPLETION_MESSAGE) {
              reduceCompletions.add(nid);
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
                let mapRoutine = (keys, values, identity, callback) => {
                  // get everything

                  // compute map on payload
                  results = [];
                  // TODO: may need to make this async-friendly
                  for (let i = 0; i < keys.length; i++) {
                    let mapped = configuration.map(keys[i], values[i]);

                    // mapped can give multiple outputs
                    for (let [key, value] of Object.entries(mapped)) {
                      results.push({key: key, value: value});
                    }
                  }

                  // compact results of map
                  compactedResults = configuration.compact(results);
                  results = [];
                  for (let [key, value] of Object.entries(compactedResults)) {
                    results.push({key: key, value: value});
                  }

                  console.log("RESULTS OF MAP: ", results);

                  // shuffle results of map in a distributed way
                  // grouping happens at the same time with append
                  let shuffler = (obj, callback) => {
                    let objKey = `mr-mapResult-${mapReduceID}-${obj.key}`;
                    distribution[context.gid].store.append(obj.value,
                        objKey, callback);
                  };
                  looper(results, shuffler, 0, () => {
                    // setup notification
                    let remote = {service: mrNotificationServiceID,
                      method: 'notify', node: thisNode};
                    let payload = [MAP_COMPLETION_MESSAGE,
                      util.id.getNID(identity)];

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
                  mapFunc(arguments[0], arguments[1], global.distribution.
                    util.id.getNID(global.nodeConfig),
                    arguments[2]);
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
                  for (let i = 0; i < configuration.keys.length; i += setSize) {
                    keySets.push(configuration.keys.slice(i, i + setSize));
                    valueSets.push(values.slice(i, i + setSize));
                  };

                  let groupList = Object.values(group);

                  let mapSender = (i, callback) => {
                    let remote = {service: mrMapServiceID, method: 'map',
                      node: groupList[i]};
                    let payload = [keySets[i], valueSets[i]];

                    distribution.local.comm.send(payload, remote, (e, v) => {
                      if (e) {
                        console.log('Error sending map request: ', e);
                      }

                      callback(e, v);
                    });
                  };
                  let indices = Array.from({length: groupList.length},
                      (v, i) => i);
                  looper(indices, mapSender, 0);
                });
              });
        });
      });
    },
  };
};

module.exports = mr;
