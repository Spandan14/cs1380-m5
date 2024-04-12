//  ________________________________________
// / NOTE: You should use absolute paths to \
// | make sure they are agnostic to where   |
// | your code is running from! Use the     |
// \ `path` module for that purpose.        /
//  ----------------------------------------
//         \   ^__^
//          \  (oo)\_______
//             (__)\       )\/\
//                 ||----w |
//                 ||     ||


const id = require('../util/id');
const serialize = require('../util/serialization');
const path = require('path');
const fs = require('fs');

let store = {};

store.get = function(key, callback) {
  callback = callback || function() {};

  if (typeof key !== 'object' || !key) {
    key = {
      key: key,
      gid: 'local',
    };
  }

  if (!key.key) {
    let folderPath = path.join(__dirname, '../../store/',
        id.getNID(global.nodeConfig), '/', key.gid);
    console.log(folderPath);
    fs.readdir(folderPath, function(err, files) {
      if (err) {
        callback(null, {});
      } else {
        callback(null, files);
      }
    });
    return;
  }

  let filePath = path.join(__dirname, '../../store/',
      id.getNID(global.nodeConfig) + '/' + key.gid + '/' + key.key);
  fs.readFile(filePath, 'utf8', function(err, data) {
    if (err) {
      if (err.code === 'ENOENT') {
        callback(new Error('Key not found'), null);
      } else {
        callback(err, null);
      }
    } else {
      callback(null, serialize.deserialize(data));
    }
  });
};

store.put = function(value, key, callback) {
  callback = callback || function() {};

  if (typeof key !== 'object' || !key) {
    key = {
      key: key,
      gid: 'local',
    };
  }

  if (!key.key) {
    key.key = id.getID(value);
  }

  let storeDir = path.join(__dirname, '../../store/');
  let nodeDir = storeDir + id.getNID(global.nodeConfig);
  let fileDir = nodeDir + '/' + key.gid;
  let filePath = fileDir + '/' + key.key;

  if (!fs.existsSync(storeDir)) {
    fs.mkdirSync(storeDir);
  }

  if (!fs.existsSync(nodeDir)) {
    fs.mkdirSync(nodeDir);
  }

  if (!fs.existsSync(fileDir)) {
    fs.mkdirSync(fileDir);
  }

  let options = {
    flag: 'w+',
  };

  fs.writeFile(filePath, serialize.serialize(value), options, function(err) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, value);
    }
  });
};

store.append = function(value, key, callback) {
  callback = callback || function() {};

  if (typeof key !== 'object' || !key) {
    key = {
      key: key,
      gid: 'local',
    };
  }

  if (!key.key) {
    key.key = id.getID(value);
  }

  if (!Array.isArray(value)) {
    value = [value];
  }

  store.get(key, function(err, data) {
    if (err) {
      if (err.message === 'Key not found') {
        store.put(value, key, callback);
      }
    } else {
      if (!Array.isArray(data)) {
        data = [data];
      }

      for (let i = 0; i < value.length; i++) {
        data.push(value[i]);
      }

      store.put(data, key, callback);
    }
  });
};

store.del = function(key, callback) {
  callback = callback || function() {};

  if (typeof key !== 'object' || !key) {
    key = {
      key: key,
      gid: 'local',
    };
  }

  let filePath = path.join(__dirname, '../../store/',
      id.getNID(global.nodeConfig) + '/' + key.gid + '/' + key.key);
  store.get(key, function(err, value) {
    if (err) {
      callback(err, null);
    } else {
      fs.unlink(filePath, function(err) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, value);
        }
      });
    }
  });
};

module.exports = store;
