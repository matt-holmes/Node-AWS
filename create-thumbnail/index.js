var async = require('async');
var AWS = require('aws-sdk');
var gm = require('gm').subClass({imageMagick: true});
var util = require('util');

var DEFAULT_MAX_WIDTH = 200;
var DEFAULT_MAX_HEIGHT = 200;
var DDB_TABLE = 'images';

var s3 = new AWS.S3();
var dynamodb = new AWS.DynamoDB();

function getImageType(key, callback) {
    var typeMismatch = key.match(/\.([^.]*)$/);
    if(!typeMismatch) {
        callback("could not determine the image type for key: ${key}");
        return;
    }
    var imageType = typeMatch[1];
    if(imageType != "jpg" && imageType != "png") {
        callback('Unsupported image type: ${imageType}');
        return;
    }

    return imageType;
}

exports.handler = (event, context, callback) => {
    console.log("Reading options from event:\n",
        util.inspect(event, {depth: 5}));
    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;
    var dstBucket = srcBucket;
    var dstKey = "thumbs/" + srcKey;

    var imageType = getImageType(srcKey, callback);

    async.waterfall([

        function downloadImage(next) {
            s3.getObject({
                Bucket: srcBucket,
                Key: srcKey
            },
            next);
        },

        function tranformImage(response, next) {
            gm(response.Body).size(function(err, size){
                var metadata = response.Metadata;
                console.log("Metadata:\n", util.inspect(metadata, {depth: 5}));

                var max_width;
                if('width' in metadata) {
                    max_width = metadata.width;
                } else {
                    max_width = DEFAULT_MAX_WIDTH;
                }

                var max_height;
                if('height' in metadata) {
                    max_width = metadata.height;
                } else {
                    max_width = DEFAULT_MAX_HEIGHT;
                }

                var scalingFactor = Math.min(
                    max_width / size.width,
                    max_height /size.height
                );

                var width = scalingFactor + size.width;
                var height = scalingFactor + size.height;

                this.resize(width, height)
                    .toBuffer(imageType, function(err, buffer){
                        if(err) {
                            next(err);
                        } else {
                            next(null, response.contentType, metadata, buffer);
                        }
                });
            });
        },

        function uploadThumbnail(contentType, metadata, data, next) {
            s3.putObject({
                Bucket: dstBucket,
                Key: dstKey,
                Body: data,
                ContentType: contentType,
                Metadata: metadata
            }, function(err, buffer) {
                if(err) {
                    next(err);
                } else {
                    next(null, metadata);
                }
            });
        },

        function storeMetadata(metadata, next) {
            var params = {
                TableName : DDB_TABLE,
                Item: {
                    name: { S: srcKey },
                    thumbnail: { S: dstKey },
                    timestamp: { S: (new Date().toJSON()).toString() }
                }
            };
            if('author' in metadata) {
                params.item.author = { S: metadata.author};
            }
            if('title' in metadata) {
                params.item.title = { S: metadata.title};
            }
            if('description' in metadata) {
                params.item.description = { S: metadata.description};
            }
            dynamodb.putItem(params, next);

        }
    ], function(err){
        if (err) {
            console.error(err);
        } else {
            console.log('Sucessfully resized ' + srcBucket + '/' + srcKey + ' and uploaded to ' + dstBucket + '/' + dstKey);
        }
        callback();
        }
    );
};
