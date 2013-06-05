var common     = require('../common');
var assert     = require('assert');

//definition assumes that the last key listed in keys array is the serial or auto-increment column
common.createKeysWithSerialModelTable = function (table, db, keys, cb) {
	var keystrs = [];
	
	switch (this.protocol()) {
		case "postgres":
		case "redshift":
			
			for(var i = 0; i<keys.length; i++) {
				if (i < keys.length-1) {
					keystrs[i] = keys[i] + " BIGINT NOT NULL";
				} else {
					keystrs[i] = keys[i] + " SERIAL";
				}
			}
			db.query("CREATE TEMPORARY TABLE " + table + " (" + keystrs.join(", ") + ", name VARCHAR(100) NOT NULL, PRIMARY KEY (" + keys.join(", ") + "))", cb);
			break;
		case "sqlite":
			for(var i = 0; i<keys.length; i++) {
				if (i < keys.length-1) {
					keystrs[i] = keys[i] + " BIGINT NOT NULL";
				} else {
					keystrs[i] = keys[i] + " AUTO_INCREMENT";
				}
			}
			db.run("DROP TABLE IF EXISTS " + table, function () {
				db.run("CREATE TEMPORARY TABLE " + table + " (" + keystrs.join(", ") + ", name VARCHAR(100) NOT NULL, PRIMARY KEY (" + keys.join(", ") + "))", cb);
			});
			break;
		default:
			for(var i = 0; i<keys.length; i++) {
				if (i < keys.length-1) {
					keystrs[i] = keys[i] + " BIGINT NOT NULL";
				} else {
					keystrs[i] = keys[i] + " BIGINT NOT NULL AUTO_INCREMENT";
				}
			}			
			db.query("CREATE TEMPORARY TABLE " + table + " (" + keystrs.join(", ") + ", name VARCHAR(100) NOT NULL, PRIMARY KEY (" + keys.join(", ") + ")) ENGINE=MyISAM", cb);
			break;
	}
};

common.createConnection(function (err, db) {
	
	common.createKeysWithSerialModelTable('test_multikey_with_serial_base', db.driver.db, [ 'id1', 'id2' ], function (err) {
		
		if(err) {
			throw err;
		}
		
		var TestModel = db.define('test_multikey_with_serial_base', common.getModelProperties(), {
			keys  : [ 'id1', 'id2' ],
			cache : false
		});
		
		var data = [{ name: 'John Doe', id1: 1 },{ name: 'Jane Doe', id1: 1 },{ name: 'Max Doe', id1: 2}];			
							
		TestModel.create( data, function(err, items) {
			if (err) throw err;			
			assert(items[0].id2, 1);			
			assert(items[1].id2, 2);
			assert(items[2].id2, 1);
			db.close();			
		});
	});
	
});