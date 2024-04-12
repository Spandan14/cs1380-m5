let store = (config) => {
  let context = {};
  const util = global.distribution.util;
  context.gid = config.gid || 'all';
  context.hash = config.hash || util.id.consistentHash;

  return {
    'get': function(key, callback) {
      callback = callback || function() {};

      distribution.local.groups.get(context.gid, (e, v) => {
        if (e) {
          return callback(e, null);
        }

        // accesses the node with the NID that is responsible for the key
        let nodeToCheck = v[context.hash(util.id.getID(key), Object.keys(v))];

        key = {
          key: key,
          gid: context.gid,
        };

        let payload = [key];
        let remote = {
          node: nodeToCheck,
          service: 'store',
          method: 'get',
        };

        if (!key.key) {
          distribution[context.gid].comm.send(payload, remote, (e, v) => {
            callback(e, Object.values(v).flat());
          });
          return;
        }

        distribution.local.comm.send(payload, remote, (e, v) => {
          callback(e, v);
        });
      });
    },
    'put': function(value, key, callback) {
      callback = callback || function() {};

      distribution.local.groups.get(context.gid, (e, v) => {
        if (e) {
          return callback(e, v.flat());
        }

        if (!key) {
          key = util.id.getID(value);
        }

        // accesses the node with the NID that is responsible for the key
        let nodeToCheck = v[context.hash(util.id.getID(key), Object.keys(v))];

        key = {
          key: key,
          gid: context.gid,
        };

        let payload = [value, key];
        let remote = {
          node: nodeToCheck,
          service: 'store',
          method: 'put',
        };

        distribution.local.comm.send(payload, remote, (e, v) => {
          callback(e, v);
        });
      });
    },
    'append': function(value, key, callback) {
      callback = callback || function() {};

      distribution.local.groups.get(context.gid, (e, v) => {
        if (e) {
          return callback(e, null);
        }

        if (typeof key !== 'object' || !key) {
          key = {
            key: key,
            gid: context.gid,
          };
        }

        if (!key.key) {
          key.key = util.id.getID(value);
        }

        let nodeToCheck = v[context.hash(util.id.getID(key), Object.keys(v))];

        let payload = [value, key];
        let remote = {
          node: nodeToCheck,
          service: 'store',
          method: 'append',
        };

        distribution.local.comm.send(payload, remote, (e, v) => {
          callback(e, v);
        });
      });
    },
    'del': function(key, callback) {
      callback = callback || function() {};

      distribution.local.groups.get(context.gid, (e, v) => {
        if (e) {
          return callback(e, null);
        }

        // accesses the node with the NID that is responsible for the key
        let nodeToCheck = v[context.hash(util.id.getID(key), Object.keys(v))];

        key = {
          key: key,
          gid: context.gid,
        };

        let payload = [key];
        let remote = {
          node: nodeToCheck,
          service: 'store',
          method: 'del',
        };

        distribution.local.comm.send(payload, remote, (e, v) => {
          callback(e, v);
        });
      });
    },
    'reconf': function(oldGroup, callback) {
      callback = callback || function() {};

      distribution[context.gid].store.get(null, (e, v) => {
        if (Object.keys(e).length > 0) {
          return callback(e, null);
        }

        let allKeys = v;
        distribution.local.groups.get(context.gid, (e, v) => {
          if (e) {
            return callback(e, null);
          }

          let newGroup = v;
          let toRelocate = {};
          for (let i = 0; i < allKeys.length; i++) {
            key = allKeys[i];

            let oldNode = oldGroup[context.hash(util.id.getID(key),
                Object.keys(oldGroup))];
            let newNode = newGroup[context.hash(util.id.getID(key),
                Object.keys(newGroup))];

            if (oldNode !== newNode) {
              toRelocate[key] = {
                oldNode: oldNode,
                newNode: newNode,
              };
            }
          }

          let i = 0;
          const loopingCaller = (err, value) => {
            let key = Object.keys(toRelocate)[i];
            let oldNode = toRelocate[key].oldNode;
            let newNode = toRelocate[key].newNode;

            key = {
              key: key,
              gid: context.gid,
            };

            let oldPayload = [key];
            let getRemote = {
              node: oldNode,
              service: 'store',
              method: 'get',
            };
            let delRemote = {
              node: oldNode,
              service: 'store',
              method: 'del',
            };

            distribution.local.comm.send(oldPayload, getRemote, (e, v) => {
              if (e) {
                return callback(e, null);
              }
              distribution.local.comm.send(oldPayload, delRemote, (e, v) => {
                if (e) {
                  return callback(e, null);
                }

                let newPayload = [v, key];
                let putRemote = {
                  node: newNode,
                  service: 'store',
                  method: 'put',
                };

                distribution.local.comm.send(newPayload, putRemote, (e, v) => {
                  if (e) {
                    return callback(e, null);
                  }

                  i++;
                  if (i < Object.keys(toRelocate).length) {
                    loopingCaller(null, null);
                  } else {
                    callback(null, 'reconf complete');
                  }
                });
              });
            });
          };

          loopingCaller(null, null);
        });
      });
    },
  };
};

module['exports'] = store;
