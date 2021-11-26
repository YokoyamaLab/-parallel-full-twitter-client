import { workerData, parentPort, MessageChannel } from 'worker_threads'
import { io } from "socket.io-client";
import * as fs from 'fs';
import path from 'path';
import lz4 from 'lz4';
import readline from 'readline';
import { DateTime, Interval } from 'luxon';
import jsonMask from 'json-mask';

export default async ({ port, query, file, fileN, startTime, client }) => {
    const startTimeFile = DateTime.now();
    const decoder = lz4.createDecoderStream();
    const input = fs.createReadStream(file);
    const reader = readline.createInterface({ input: input.pipe(decoder), crlfDelay: Infinity });
    let nLine = 0
    let hit = 0;
    let error = 0;
    let hitTweets = [];
    for await (const line of reader) {
        if (line.trim() == "") {
            error++
            continue;
        }
        nLine++;
        //if (nLine % 100000 == 0) { port.postMessage({ "type": "message", client: client, message: "Line: " + nLine }) };
        const tweet = JSON.parse(line);
        if (!tweet.hasOwnProperty("text")) {
            error++;
            continue;
        }
        if (!Array.isArray(query.keywords)) {
            query.keywords = [query.keywords];
        }
        if (query.keywords.some((keyword) => {
            return tweet.text.toUpperCase().indexOf(keyword.toUpperCase()) > -1;
        })) {
            const rtn = jsonMask(tweet, query.mask);
            hit++;
            hitTweets.push(rtn);
            //port.postMessage({ "type": "hit", client: client, tweet: rtn });
        }
    }
    const finishTimeFile = DateTime.now();
    const execTimeFile = finishTimeFile.diff(startTimeFile, 'seconds');
    //port.postMessage({ "type": "message", client: client, message: "File: " + file });
    port.postMessage({
        "type": "finish",
        client: client,
        startTimeFile: startTimeFile.toString(),
        finishTimeFile: finishTimeFile.toString(),
        execTimeFile: execTimeFile.seconds,
        nTweets: nLine,
        nHits: hit,
        nError: error,
        file: file,
        fileN: fileN,
        startTime: startTime,
        monitor: query.monitor,
        queryId: query.queryId,
        result: hitTweets
    });
};

/*
export default ({ port, file, query }) => {
    const startTime = DateTime.now();
    const decoder = lz4.createDecoderStream();
    const input = fs.createReadStream(file);
    const reader = readline.createInterface({ input: input.pipe(decoder), crlfDelay: Infinity });
    let nLine = 0
    let hit = 0;
    let error = 0;
    console.log("scan start", file);

    for await (const line of reader) {
        if (line.trim() == "") {
            continue;
        }
        nLine++;
        if (nLine % 100000 == 0) { console.log("Line", nLine) };
        const tweet = JSON.parse(line);
        try {
            if (!Array.isArray(query.keywords)) {
                query.keywords = [query.keywords];
            }
            if (query.keywords.some((keyword) => {
                return tweet.text.toUpperCase().indexOf(keyword.toUpperCase()) > -1;
            })) {
                const rtn = jsonMask(tweet, query.mask);
                hit++;
                //port.postMessage(rtn);
            }
        } catch (e) {
            continue;
        }
    }
    //console.log("scan finish:", hit, "hits", error, "errors");
    const finishTime = DateTime.now();
    const diff = finishTime.diff(startTime, 'seconds');
    //console.log(diff.seconds);
    return { hit: hit, error: error, time: diff.seconds };
};

*/