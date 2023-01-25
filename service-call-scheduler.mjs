import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import { SFNClient, StartExecutionCommand } from "@aws-sdk/client-sfn";
import { randomUUID } from 'crypto';
const docClient = new DynamoDBClient ();
const client = new SFNClient();

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
    return arr.map (obj => {
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

// finally create records
export async function createRecord (item) {
    console.log (JSON.stringify (marshall(item), null, 2))
    return docClient.send (new PutItemCommand ({
        TableName: process.env.TableName,
        Item: marshall (item)
    }));
}

export const handler = async(event) => {
    const {calls} = JSON.parse (event.body);
    const schedule = {
        scheduleId: randomUUID (),
        callIndex: 0,
        isDone: false,
        calls,
        responses: []
    }
    const input = JSON.stringify ({scheduleId: schedule.scheduleId, callIndex: 0, isDone: false, call: calls [0]});
    await createRecord (schedule);
    const command = new StartExecutionCommand({
        input,
        stateMachineArn: 'arn:aws:states:us-east-1:842200175734:stateMachine:Service-Call-Scheduler'
    });
    await client.send(command);
    return {
        statusCode: 201,
        body: 'Ok'
    };
};
