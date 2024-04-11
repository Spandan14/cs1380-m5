const mr = function(config) {
  let context = {};
  context.gid = config.gid || 'all';
  const util = global.distribution.util;

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      callback = callback || function() {};

      // id gen
      const date = Date.now().toString(36);
      const rand = Math.random().toString(36).substring(2, 9);
      let mapReduceID = `mr-${date}-${rand}`;

      // setup notification service
      let mrNotificationService = {};
      mrNotificationService.notify = (msg) => {
        console.log('NOTIFICATION RECEIVED: ', msg);
      };
      const mrNotificationServiceID = `mr-host-notify-${mapReduceID}`;

      // register notification service
      distribution.local.routes.put(mrNotificationService,
          mrNotificationServiceID, (e, v) => {
            if (e) {
              console.log('Error registering notification service: ', e);
              // callback(e, v);
            }

            // setup node service
            let thisNode = global.nodeConfig;
            let talkToLeader = (identity) => {
              let remote = {service: mrNotificationServiceID,
                method: 'notify', node: thisNode};
              // global.nodeConfig in this scope is the remote node?

              let payload = [`hello from 
                ${identity.ip}:${identity.port}!`];
              console.log('CHILD is preparing to talk to daddy.');
              distribution.local.comm.send(payload, remote, (e, v) => {
                if (e) {
                  console.log('Error sending message to leader: ', e);
                }
                callback(e, v);
              });
            };

            let talkRPC = util.wire.createRPC(util.wire.toAsync(talkToLeader));
            let talkFuncString = `
              let talkFunc = ${talkRPC.toString()};
              talkFunc(global.nodeConfig, () => {});
              arguments[0]();
            `;
            let totalTalkFunction = Function(talkFuncString);

            const mrRemoteTalkServiceID = `mr-remote-talk-${mapReduceID}`;

            // now register services remotely on other nodes in the group
            let remote = {service: 'routes', method: 'put'};
            let payload = [{talk: totalTalkFunction}, mrRemoteTalkServiceID];

            distribution[context.gid].comm.send(payload, remote, (e, v) => {
              if (e) {
                console.log('Error setting up talking system: ', e);
                // callback(e, v);
              }

              // call the talk service and hope it works
              let remote = {service: mrRemoteTalkServiceID, method: 'talk'};
              let payload = [];

              distribution[context.gid].comm.send(payload, remote, (e, v) => {
                if (e) {
                  console.log('Error telling child to send message: ', e);
                  // callback(e, v);
                }

                // no cleanup yet
                console.log('Made it to the end of MapReduce Test.');
                callback(null, {message: 'MapReduce Test Complete.'});
              });
            });
          });
    },
  };
};

module.exports = mr;
