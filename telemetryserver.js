const express = require('express');
const Router = require('express-promise-router');
const cors = require('cors');
//const { connected } = require('process');
const PORT = process.env.PORT || 5000;
const short = require('short-uuid');
const { Pool } = require('pg');

//#region Initialization

// HTTP Response Status Codes (named for legibility)
const HTTP_SUCCESS = 200;
const HTTP_BAD_REQUEST = 400;
const HTTP_INTERNAL_ERROR = 500;

// Pool of client objects to interact with database asynchronously,
// so we can have multiple requests in flight without deadlocking server.
const pool = new Pool({
    max: 10,
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});


/**
 * @typedef {object} SessionInfo
 * @property {string} table Sanitized table name
 * @property {number} id Session index        
 * @property {string} version Game build version identifier
 * @property {string} section Name of the initial/default section
 * @property {number} lastAccess Timestamp of last message from this session 
 *                               (milliseconds since UTC epoch)
 * @property {number} nextSequence Next expected message index (last handled message + 1)
 */

// Cache of session objects.
/**@type {object.<short.SUUID, SessionInfo>} */
const sessions = {};

// Database connection initialization routine.
(async () => {

    pool.on('error', (err, client) => {
        console.error('Unexpected error on idle client', err);
        process.exit(-1);
    });   
    
    const client = await pool.connect();
    try {
        // Set to true when clearing specific DBs.
        const reset = false;

        if (reset) {
            await client.query('BEGIN');
            await client.query('DROP TABLE testing, gregdoug');
            await client.query('COMMIT');       
            
            console.log('Cleared old databases.');
        }

    } catch (e) {
        await client.query('ROLLBACK');
        throw e;
    } finally {
        client.release();
    }

    console.log('Server is up, databases are ready.');
})().catch(e => console.error(`Error during initialization: ${e.stack}`));
//#endregion

//#region Data Validation
/**
 * @param {any} integer
 * @returns {boolean} True if the data is an integer.
 */
function isValidInteger(integer) {
    return !isNaN(integer) && Number.isInteger(integer);
}

/**
 * @param {any} text 
 * @returns {boolean} True if the data is a string and is non-empty.
 */
function isValidString(text) {
    return (text != undefined && (typeof text === 'string' ||  (text instanceof String)) && text);
}

/**
 * 
 * @param {any} timecode 
 * @returns {Date} (possibly invalid) converted Date object
 */
function dateOrInvalid(timecode) {
    if (isValidInteger(timecode) || isValidString(timecode)) {
        return new Date(timecode);
    }

    return new Date(NaN);
}

/**
 * @param {string} text 
 * @returns {boolean} True if the provided string starts with a lowercase alphabetical character.
 */
function isValidIdentifier(text) {
    if (!text) return false;
    const first = text.charCodeAt(0);
    return (first >= 'a'.charCodeAt(0)) && (first <= 'z'.charCodeAt(0));
}

/**
 * 
 * @param {any} text 
 * @returns {string} Input text with whitespace removed, or '?' if the input was not a string or only whitespace.
 */
function textOrPlaceholder(text) {
    if (isValidString(text)) {
        const trimmed = text.trim();
        if (trimmed) return trimmed;
    }
    return '?';
}
//#endregion

/**
 * Sanitize a string for use as a SQL table name.
 * @param {string} name 
 * @returns {string} Canonical SQL-safe version. 
 */
function toTableName(name) {
    return name.toLowerCase().replace(/[\s-]/g, '_').replace(/[.,';:]/g, '');
}

/**
 * Checks whether a user provided the correct secret.
 * (This is a defense against accidentally writing to the wrong table,
 *  not true protection of any kind of sensitive data)
 * @param {string} userName 
 * @param {string} userSecret 
 * @returns {boolean} True if the user is authenticated.
 */
function authenticate(userName, userSecret) {
    return process.env[`user_${userName}`.toUpperCase()] === userSecret;
}

//#region Database Transactions
/**
 * 
 * @param {import('pg').PoolClient} client Pool client to execute DB transaction.
 * @param {string} tableName Name of table (assumed already pre-sanitized).
 * @returns {Promise<import('pg').QueryResult<any>>} promise to await result.
 */
function createTable(client, tableName) {

    const queryText = `CREATE TABLE ${tableName} ( 
        session     INTEGER,
        version     VARCHAR(64),
        section     VARCHAR(64),
        eventType   VARCHAR(64),
        time        TIMESTAMP,
        data        JSONB DEFAULT '{}'::jsonb NOT NULL
    );`;

    return client.query(queryText);
}

/**
 * Inserts an event into the telemetry table, with the specified data.
 * @param {import('pg').PoolClient} client 
 * @param {string} table 
 * @param {number} session 
 * @param {string} version 
 * @param {string} section 
 * @param {string} eventType 
 * @param {number} time 
 * @param {object} data 
 * @returns {Promise<import('pg').QueryResult<any>>} promise to await result.
 */
function recordEntry(client, table, session, version, section, eventType, time, data) {
    const values = [
        session,
        version,
        (section ? section : 'Default'),
        eventType,
        time,
        JSON.stringify(data)
    ];    

    const insertionText = `INSERT INTO ${table} (session, version, section, eventType, time, data) VALUES($1, $2, $3, $4, $5, $6::jsonb)`;

    return client.query(insertionText, values);
}

/**
 * @typedef {object} AuthResult Type returned by tryBeginSession
 * @property {number} sessionId - Index of this session, negative if invalid
 * @property {string} message - Text of error or welcome message
 */

/**
 * Authenticate and set up table / session index for a new connection.
 * 
 * @param {string} userName Name of team or user.
 * @param {string} userSecret Passphrase used to validate access.
 * @param {string} version Identifier for game build / update version.
 * @param {string} section Identifier for source of the message in-game (level/menu/mode/etc).
 * @param {Date} time Timestamp sent by the client.
 * @param {string} setupId Marker for how the telemetry package was configured.
 * @returns {AuthResult} Structure with session info or error message.
 */
async function tryBeginSession(sanitizedName, userSecret, version, section, time, setupId) {

    if (!authenticate(sanitizedName, userSecret)) {
        return {sessionId: -1, message: 'Failed to authenticate. Please double check that you spelled your user name and secret key correctly.'};
    }    

    let message = '';    

    let client;
    try {
        client = await pool.connect();  
        await client.query('BEGIN');
        
        const tableCheck = `SELECT EXISTS ( SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '${sanitizedName}' );`;        
        
        const tableResult = await client.query(tableCheck);
        
        let sessionId = 0;
        if (tableResult.rowCount == 0 || tableResult.rows[0].exists == false) {     
            console.log (`Found no table for ${sanitizedName} - creating from scratch as ${sanitizedName}.`);
            message = `Successfully authenticated and created new database table for ${sanitizedName}. Starting with session #${sessionId}.`;
            
            await createTable(client, sanitizedName);            
        } else {
            // Table exists. Find how many sessions already exist, and use the next available session index.
            const sessionCheck = `SELECT MAX(session) from ${sanitizedName}`;

            const sessionResult = await client.query(sessionCheck);
           
            if (sessionResult.rowCount > 0 && isValidInteger(sessionResult.rows[0].max)) sessionId = sessionResult.rows[0].max;
            
            sessionId++;

            message = `Successfully authenticated ${sanitizedName} session #${sessionId}.`;
        }        

        await recordEntry(client, sanitizedName, sessionId, version, section, 'SessionStart', time, {setup: setupId});

        await client.query('COMMIT');

        console.log(message);
        return {sessionId: sessionId, message: message};
    } catch (e) {
        message = `Error during session start for ${sanitizedName}: ${e.stack}`;
        console.error(message);

        return {sessionId: -1, message: message};
    } finally {        
        client.release();
    }
}
//endregion

//#region HTTP Request handler functions
/**
 * Endpoint to poke the server and ensure it's listening before sending telemetry data.
 * ("Eco" dyno servers on Heroku go dormant when not in use, and take some seconds to wake
 *  up again after the first request comes in, so we keep poking it till it replies successfully).
 * @param {express.Request} req 
 * @param {express.Response} res 
 */
async function awake(req, res) {
    res.status(HTTP_SUCCESS).send();
}

/**
 * Handle connection request, authenticating user and starting a telemetry session.
 * @param {express.Request} req 
 * @param {express.Response} res 
 */
async function tryConnect(req, res) {

    console.log('Authenticating... ', req.body);

    // Validate, parse, sanitize data in connection request.
    if (!req.body) { res.status(HTTP_BAD_REQUEST).send('Authentication message had no body.'); return; }
    if (!isValidString(req.body.userName)) { res.status(HTTP_BAD_REQUEST).send('Invalid user name.'); return; }
    if (!isValidString(req.body.secret)) { res.status(HTTP_BAD_REQUEST).send('Invalid secret key.'); return; }   

    /**@type {string} */
    const sanitizedName = toTableName(req.body.userName.trim());
    if (!isValidIdentifier(sanitizedName)) { res.status(HTTP_BAD_REQUEST).send('Invalid user name.'); return; }

    /**@type {string} */
    const secret = req.body.secret.trim();
    const version = textOrPlaceholder(req.body.version);
    const section = textOrPlaceholder(req.body.section);
    const setupId = textOrPlaceholder(req.body.setupId);

    const time = dateOrInvalid(req.body.timecode);
    if (isNaN(time.valueOf)) { res.status(HTTP_BAD_REQUEST).send(`Invalid timestamp. ${typeof req.body.timecode} = ${req.body.timecode}`); return; }


    console.log('timecode:', time);

    // Attempt to authenticate and start a new Telemetry Session.
    const {sessionId, message} = await tryBeginSession(sanitizedName, secret, version, section, time, setupId);

    // A negative session index indicates something went wrong. Send error message and abort.
    if (sessionId < 0) { res.status(HTTP_BAD_REQUEST).send(message); return; }

    // Generate a unique key to use for the remainder of this session.
    let sessionKey;
    do {
        sessionKey = short.generate();
    } while (sessions[sessionKey] !== undefined);

    // Cache session information (so we don't have to re-send it with every event).
    sessions[sessionKey] = {
        table: sanitizedName,
        id: sessionId,        
        version: version,
        section: section,
        lastAccess: Date.now(),
        nextSequence: 0
    };

    console.log('authentication success', sessionId);

    // Send session start confirmation.
    res.send({sessionKey: sessionKey, sessionIndex: sessionId, message: message});
}

/**
 * Handle request to log telemetry event
 * @param {express.Request} req 
 * @param {express.Response} res
 */
async function tryLogEvent(req, res) {
    // Validate, parse, sanitize data in log request.
    if (!req.body) { res.status(HTTP_BAD_REQUEST).send('Log message had no body.'); return; }
    if (!isValidString(req.body.sessionKey)) {res.status(HTTP_BAD_REQUEST).send('Invalid session key.'); return; }

    // Find session info in server cache.
    const session = sessions[req.body.sessionKey.trim()]; 
    if (session === undefined) {res.status(HTTP_BAD_REQUEST).send('Session key not found.'); return; }

    // Found valid session - keep its access timestamp fresh.
    session.lastAccess = Date.now();

    // Ensure messages are properly sequenced.
    if (!isValidInteger(req.body.sequence)) {res.status(HTTP_BAD_REQUEST).send('Invalid sequence number.'); return; }   

    // Silently reject duplicate events we've already handled. (200 = "Success" - no problem)
    // (This shouldn't happen, but it defends against bugs or ill-conceived modifications to the client).
    if (req.body.sequence < session.nextSequence) {res.status(200).send(); return;}
    
    // Decode event timestamp as Date.
    const time = dateOrInvalid(req.body.timecode);
    if (isNaN(time.valueOf)) { res.status(HTTP_BAD_REQUEST).send(`Invalid timestamp. ${typeof req.body.timecode} = ${req.body.timecode}`); return; }

    // Ensure an event type was specified.
    if (!isValidString(req.body.eventType)) {res.status(HTTP_BAD_REQUEST).send('Invalid event type.'); return; }

    // Read event specifics.
    const eventType = textOrPlaceholder(req.body.eventType);
    const section = textOrPlaceholder(req.body.section);

    // Parse data into a corresponding structure - defaulting to an empty object.
    let data = {};
    switch (typeof  req.body.data) {
    case 'string':
    case 'number':      
    case 'boolean':
        // If the data was a single literal, use the format data = {value: literal}
        data.value = req.body.data;
        break;
    case 'object':         
        if (Array.isArray(req.body.data))
            // If the data was an array, use the format data = {list: [item 1, item 2, ...]}
            data.list = req.body.data;
        else
            // For other objects, use the property structure specified in the JSON.
            data = req.body.data; 
        break;
    }
    
    // Attempt to record the event in the telemetry database table, and report any errors.
    try {
        await recordEntry(pool, session.table, session.id, session.version, section, eventType, time, data);
        session.nextSequence = req.body.sequence + 1;
    } catch (e) {
        const message = `Error logging ${eventType} for ${session.table}: ${e.stack}`;
        console.error(message);

        res.status(HTTP_INTERNAL_ERROR).send(message);
        return;
    }

    // Report success.
    res.status(200).send();
}
//#endregion

//#region Error handling
async function safeShutdown(code) {
    console.log(`Process exiting with code: ${code}`);

    await pool.end();

    console.log('Database access pool has correctly drained.');
}

process.on('beforeExit', safeShutdown);

process.on('unhandledRejection', (reason, promise) => {
    console.log('Unhandled rejection at ', promise, `reason: ${reason}`);
    process.exit(1);
});

process.on('uncaughtException', err => {
    console.log(`Uncaught Exception: ${err.message}`);
    process.exit(1);
});
//#endregion


//#region HTTP Server configuration.

// CORS (Cross-Origin Resource Sharing) controls which domains are allowed to make requests of this server.
// Here we allow common ones that show up in game distribution (itch.io and their content distribution network for web games,
// localhost and chrome extensions for playing locally off of the user's desktop)
var corsOptions = {
    origin: function (origin, callback) {
        if (!origin || origin.indexOf('itch.io/') != -1 || origin.indexOf('ssl.hwcdn.net') != -1 || origin.startsWith('http://localhost') || origin.startsWith('chrome-extension://')) {
            callback(null, true);
        } else {
            console.log(`Rejecting request from ${origin}.`);
            callback(new Error('Not allowed by CORS'));
        }
    }
};

// Set up HTTP server with JSON parsing enabled.
var app = express()
    .use(express.json());


// Configures logic for routing URL endpoints to which functions they call.
const router = new Router();
app.use(router);

router.get('/awake', cors(corsOptions), awake)
    .options('/awake', cors(corsOptions));

router.post('/connect', cors(corsOptions), tryConnect)
    .options('/connect', cors(corsOptions));

router.post('/log', cors(corsOptions), tryLogEvent)  
    .options('/log', cors(corsOptions));


// Sets the server to begin listening for requests.
app.listen(PORT, () => console.log(`Listening on ${ PORT }`));
//#endregion

//#region Clean-up
// Periodically purge the cache of telemetry sessions we haven't heard from since the last purge.
// This keeps server memory use bounded.

// Run a purge every 30 minutes.
const SESSION_INTERVAL = ((minutes) => {return minutes * 60 * 1000;})(30);

function cleanupOldSessions() {
    const threshold = Date.now() - SESSION_INTERVAL;
    let deleted = 0;
    let remaining = 0;
    for (const key of Object.keys(sessions)) {
        if (sessions[key].lastAccess < threshold) {
            delete sessions[key];
            deleted++;
        } else {
            remaining++;
        }        
    }

    console.log(`Finished session cleanup: ${deleted} old sessions cleared, ${remaining} active sessions remain.`);
}

setInterval(cleanupOldSessions, SESSION_INTERVAL);
//#endregion