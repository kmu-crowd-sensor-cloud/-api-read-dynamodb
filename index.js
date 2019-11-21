const AWS = require("aws-sdk");

if (process.env.NODE_ENV === 'devel') {
    const config = require('../config.js');
    AWS.config.update({
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
        region: "ap-northeast-2",
    });
} else {
    AWS.config.update({
        region: "ap-northeast-2",
    });
}

exports.handler = async (gevent, context) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    const event = gevent.queryStringParameters || {};
    let count = event.count || 25;
    const html = gevent.path.endsWith(".html");
    const deviceName = event.device || event.name;
    if (count < 5) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": "요청 인자 count가 너무 작습니다."
            })
        };
    }
    if (count > 1000) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": "요청 인자 count가 너무 큽니다."
            })
        };
    }
    let db_query = (params, callback) => {
        docClient.query(params, callback);
    };
    const params = {
        TableName: "CrowdSensorCloudData",
        ScanIndexForward: false,
        KeyConditionExpression: "#n = :device",
        ExpressionAttributeNames: {
            "#n": "device",
        },
        ExpressionAttributeValues: {
            ":device": deviceName || '0',
        },
        Limit: count
    };
    if (deviceName) {
        if (event.start || event.end) {
            count = event.count || 1000;
            params["Limit"] = count;
        }
        if (event.start) {
            params["KeyConditionExpression"] = "#n = :device and #t >= :start";
            params["ExpressionAttributeNames"]["#t"] = "timestamp";
            params["ExpressionAttributeValues"][":start"] = parseInt(event.start);
            if (event.end) {
                params["KeyConditionExpression"] = "#n = :device and #t between :start and :end";
                params["ExpressionAttributeNames"]["#t"] = "timestamp";
                params["ExpressionAttributeValues"][":end"] = parseInt(event.end);
            }
        } else if (event.end) {
            params["KeyConditionExpression"] = "#n = :device and #t <= :start";
            params["ExpressionAttributeNames"]["#t"] = "timestamp";
            params["ExpressionAttributeValues"][":end"] = parseInt(event.end);
        }
    } else {
        params["IndexName"] = "rule-timestamp-index";
        params["KeyConditionExpression"] = "#n = :rule";
        params["ExpressionAttributeNames"] = {
            "#n": "rule"
        };
        params["ExpressionAttributeValues"][":rule"] = "AirQualityCollectSQS";
        delete params["ExpressionAttributeValues"][":device"];
        if (event.start) {
            params["KeyConditionExpression"] = "#n = :rule and #t >= :start";
            params["ExpressionAttributeNames"]["#t"] = "timestamp";
            params["ExpressionAttributeValues"][":start"] = parseInt(event.start);
            if (event.end) {
                params["KeyConditionExpression"] = "#n = :rule and #t between :start and :end";
                params["ExpressionAttributeNames"]["#t"] = "timestamp";
                params["ExpressionAttributeValues"][":end"] = parseInt(event.end);
            }
        } else if (event.end) {
            params["KeyConditionExpression"] = "#n = :rule and #t <= :start";
            params["ExpressionAttributeNames"]["#t"] = "timestamp";
            params["ExpressionAttributeValues"][":end"] = parseInt(event.end);
        }
    }

    try {
        return await new Promise((resolve, reject) => {
            db_query(params, function (err, data) {
                if (err) {
                    reject("Unable to query. Error: " + JSON.stringify(err, null, 2));
                } else if (deviceName && data.Items.length === 0) {
                    reject(`${deviceName}의 자료가 없습니다.`);
                } else if (data.Items.length === 0) {
                    reject(`검색된 자료가 없습니다.`);
                } else {
                    let cnt = 0;
                    let results = [];
                    let rows = [];
                    data.Items.sort(function (a, b) {
                        return -1 * (a.timestamp < b.timestamp ? -1 : a.timestamp === b.timestamp ? 0 : 1);
                    }).forEach(function (data) {
                        if (cnt < count) {
                            if (data['rule']) {
                                delete data['rule'];
                            }
                            results.push(data);
                            if (html) {
                                rows.push(`<tr><td>${data.device}</td><td>${data.temperature}</td><td>${data.humidity}</td><td>${data.pm10}</td><td>${data.pm25}</td><td>${new Date(data.timestamp + (9 * 60 * 60 * 1000)).toISOString().replace('T', ' ').replace(/\..+/, ' KST')}</td></tr>`)
                            }
                        }
                        cnt++;
                    });
                    if (!html) {
                        if (process.env.NODE_ENV === 'devel') {
                            console.log(results);
                        }
                        resolve({
                            statusCode: 200,
                            headers: {
                                "Cache-Control": "max-age=300",
                            },
                            body: JSON.stringify({
                                "status": "success",
                                "count": cnt,
                                "results": results
                            })
                        });
                    } else {
                        resolve({
                            statusCode: 200,
                            headers: {
                                "Content-Type": "text/html",
                                "Cache-Control": "max-age=300",
                            },
                            body:
                                `<html><head><title>Air@Home&Mobile</title>` +
                                `<meta http-equiv="Content-type" content="text/html; charset=utf-8">` +
                                `<meta name="viewport" content="width=device-width,initial-scale=1">` +
                                `<link rel="stylesheet" type="text/css" href="//cdn.datatables.net/1.10.19/css/jquery.dataTables.css">` +
                                `<script type="text/javascript" language="javascript" src="https://code.jquery.com/jquery-3.3.1.js"></script>` +
                                `<script type="text/javascript" charset="utf8" src="//cdn.datatables.net/1.10.19/js/jquery.dataTables.js"></script>` +
                                `<script type="text/javascript" charset="utf8">$(document).ready(function() {$('#air-table').DataTable({"language": {"url":"//cdn.datatables.net/plug-ins/9dcbecd42ad/i18n/Korean.json"},"order":[[5,"desc"]]});});</script>` +
                                `</head>` +
                                `<body><h1 style="text-align: center">KMU Crowd Sensor Cloud</h1>` +
                                `<h3 style="text-align: center"><b>Air@Home&Mobile</b></h1>` +
                                `<table id="air-table" class="display" style="width:100%;">` +
                                `<thead><tr><th>이름(센서)</th><th>온도</th><th>습도</th><th>미세먼지(PM 10)</th><th>초미세먼지(PM 2.5)</th><th>측정시간</th></thead>` +
                                `<tbody>${rows.join("")}</tbody></table></body></html>`
                        });
                    }
                }
            });
        });
    } catch (err) {
        if (process.env.NODE_ENV === 'devel') {
            console.error(err);
        }
        return {
            statusCode: 400,
            headers: {
                "Cache-Control": "max-age=300",
            },
            body: JSON.stringify({
                "status": "error",
                "error": (typeof err === 'string') ? err : JSON.stringify(err)
            })
        };
    }
};


if (process.env.NODE_ENV === 'devel') {
    exports.handler({
        "body": "eyJ0ZXN0IjoiYm9keSJ9",
        "resource": "/{proxy+}",
        "path": "/path/to/resource",
        "httpMethod": "POST",
        "isBase64Encoded": true,
        "queryStringParameters": {
            // "name": "AirSensor20133219",
            "start": "1",
            // "end": "1558430580016"
        },
        "pathParameters": {
            "proxy": "/path/to/resource"
        },
        "stageVariables": {
            "baz": "qux"
        },
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "max-age=0",
            "CloudFront-Forwarded-Proto": "https",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-Mobile-Viewer": "false",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Tablet-Viewer": "false",
            "CloudFront-Viewer-Country": "US",
            "Host": "1234567890.execute-api.ap-northeast-2.amazonaws.com",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Custom User Agent String",
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https"
        },
        "requestContext": {
            "accountId": "123456789012",
            "resourceId": "123456",
            "stage": "prod",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "requestTime": "09/Apr/2015:12:34:56 +0000",
            "requestTimeEpoch": 1428582896000,
            "identity": {
                "cognitoIdentityPoolId": null,
                "accountId": null,
                "cognitoIdentityId": null,
                "caller": null,
                "accessKey": null,
                "sourceIp": "127.0.0.1",
                "cognitoAuthenticationType": null,
                "cognitoAuthenticationProvider": null,
                "userArn": null,
                "userAgent": "Custom User Agent String",
                "user": null
            },
            "path": "/prod/path/to/resource",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "apiId": "1234567890",
            "protocol": "HTTP/1.1"
        }
    });
}
