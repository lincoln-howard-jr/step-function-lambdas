import {UpdateCommand} from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient, GetItemCommand, AttributeValue } from '@aws-sdk/client-dynamodb';
import {unmarshall} from '@aws-sdk/util-dynamodb';
import {request} from 'https';
const docClient = new DynamoDBClient ();

const callService = async (url, method="GET", headers={}, body=undefined) => new Promise ((resolve => {
    const data = [];
    const response = {}
    const req = request (url, {
        method,
        headers
    }, res => {
        response.statusCode = res.statusCode;
        
        res.on ('data', d => data.push (d))
        res.on ('end', () => {
            response.body = data.join ('');
            resolve (response);
        })
    })
    req.on ('error', () => {
        resolve (response);
    })
    if (body !== undefined) req.write (JSON.stringify (body))
    req.end ();
}))

// convert value types to attribute value
function valueMarshall (obj) {
    if (typeof obj === 'string') return {
        S: obj
    }
    if (typeof obj === 'number') return {
        N: JSON.stringify (obj)
    }
    if (typeof obj === 'boolean') return {
        BOOL: obj
    }
    return {
        NULL: true
    }
}
// convert an array to attribute value array
function listMarshall (arr) {
    return arr.map<AttributeValue> (obj => {
        if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean') return valueMarshall (obj)
        if (Array.isArray (obj)) return {
            L: listMarshall (arr)
        }
        if (typeof obj === 'object') return {
            M: marshall (obj)
        }
        return {
            NULL: true
        }
    })
}
// actually marshall and create record for an object
function marshall (obj, key) {
    if (!key) return Object.keys (obj).reduce ((ret, k) => {
        return {
            ...ret,
            ...marshall (obj [k], k)
        }
    }, {})
    if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean') return {
        [key]: valueMarshall (obj)
    } 
    if (Array.isArray (obj)) return {
        [key]: {
            L: listMarshall (obj)
        }
    }
    if (typeof obj === 'object') return marshall (obj);
    return {
        [key]: {
            NULL: true
        }
    }
}

async function getRecord (id) {
    try {
        const command = new GetItemCommand ({
            TableName: process.env.TableName,
            Key: {
                scheduleId: {
                    S: id
                }
            }
        })
        const response = await docClient.send (command)
        return response.Item ? (unmarshall (response.Item)) : null;
    } catch (err) {
        console.log (err);
        throw err;
    }
}

function createUpdateExpression (obj, hashKey) {
    const keys =  Object.keys (obj).filter (k => k !== hashKey);
    return {
        UpdateExpression: 'set ' + keys.map (k => `${k} = :${k}`).join (','),
        ExpressionAttributeValues: keys.reduce ((ret, k) => {
            return {
                ...ret,
                [`:${k}`]: obj [k]
            }
        }, {})
    }
}

async function updateRecord (obj, hashKey) {
    if (!obj [hashKey]) throw 'Cannot update record without a hash key';
    const command = new UpdateCommand ({
        TableName: process.env.TableName,
        Key: {
            [hashKey]: obj [hashKey]
        },
        ...createUpdateExpression (obj, hashKey)
    })
    return docClient.send (command)
}
export const handler = async (event) => {
    const schedule = await getRecord (event.scheduleId);
    if (schedule.calls.length === schedule.responses.length) return {
        isDone: true
    }
    const res = await callService (event.call.url, event.call.method, event.call.headers, event.call.body);
    const callIndex = event.callIndex + 1;
    schedule.callIndex = callIndex;
    schedule.responses.push (res);
    await updateRecord(schedule, 'scheduleId');
    
    if (callIndex === schedule.calls.length) return {
        isDone: true
    }
    
    return {
        isDone: false,
        scheduleId: event.scheduleId,
        callIndex,
        call: schedule.calls [callIndex]
    }
};
