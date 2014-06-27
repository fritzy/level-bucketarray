function LevelBucketArray(db_, ba_opts) {
    function DB() {}
    DB.prototype = db_;
    var db = new DB();
    db.parent = db_;

    ba_opts = ba_opts || {};
    ba_opts.sep = ba_opts.sep || '!';

    db._bucketArrayJoin = function (bucket) {
        if (Array.isArray(bucket)) {
            bucket = bucket.join(ba_opts.sep);
        }
        return bucket;
    };

    db.put = function (key, value, opts, callback) {
        if (typeof callback === 'undefined') {
            callback = opts;
            opts = {};
        }
        opts.bucket = db._bucketArrayJoin(opts.bucket);
        return db.parent.put(key, value, opts, callback);
    };

    db.get = function (key, opts, callback) {
        if (typeof callback === 'undefined') {
            callback = opts;
            opts = {};
        }
        opts.bucket = db._bucketArrayJoin(opts.bucket);
        return db.parent.get(key, opts, callback);
    };

    db.del = function (key, opts, callback) {
        if (typeof callback === 'undefined') {
            callback = opts;
            opts = {};
        }
        opts.bucket = db._bucketArrayJoin(opts.bucket);
        return db.parent.del(key, opts, callback);
    };

    db.createReadStream = function (opts) {
        opts.bucket = db._bucketArrayJoin(opts.bucket);
        return db.parent.createReadStream(opts);
    };

    db.createWriteStream = function (opts) {
        opts.bucket = db._bucketArrayJoin(opts.bucket);
        return db.parent.createWriteStream(opts);
    };

    return db;
}

module.exports = LevelBucketArray;
