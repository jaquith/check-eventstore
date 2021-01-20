#!/usr/bin/env node

// reference: https://medium.freecodecamp.org/writing-command-line-applications-in-nodejs-2cf8327eee2
'use strict';

const fs = require('fs-extra')
const zlib = require('zlib')
const program = require('commander')

const DataFrame = require('dataframe-js').DataFrame;

// folder to pull .gz files from
const folder = './samples/';

/*
program
  .version('0.0.1')
  .command('check-eventstore-folder [folder]')
  .description('')
  .action()
  
program.parse(process.argv)
*/

let allRecords = []
const keyCollector = {}

let myKeys = [
  'udo_tealium_account',
  'udo_tealium_profile',
  'udo_tealium_environment',
  'udo_tealium_event',
  'dom_domain',
  'dom_url',
  'pageurl_full_url',
  'dom_query_string',
  'eventid',
  'udo_ut_version'
]

fs.readdir(folder, (err, files) => {
  if (err) {
    console.log(err);
    return;
  }
  files.forEach((file, i, arr) => {
    var isGZ = /\.gz$/.test(file);
    if (!isGZ) {
      console.log("--- SKIPPING FILE " + folder + file + " ----" )
      return;
    }
    console.log("--- READING FILE " + folder + file + " ----" )
    // this should be OK because the files are relatively small by design in EventStore, and it makes the code simpler
    let data = fs.readFileSync(folder + file);
    var treeStream = Buffer.from(data);
    let buffer = zlib.unzipSync(treeStream);
    var string = buffer.toString();
    var events = string.split("\n").map(el => JSON.parse(el))
    events.forEach((event) => {
      allRecords.push(event)
      let eventKeys = Object.keys(event)
      eventKeys.forEach((eventKey) => {
        if (!eventKey) return
        keyCollector[eventKey] = true
      })
    });
  });

  console.log(allRecords.length)

  // for debugging, could be removed
  let allKeys = Object.keys(keyCollector)
  console.log(allKeys)

  const df = new DataFrame(allRecords, myKeys);

  df.show();
  df.toJSON(true, '/Users/calebjaquith/.git/check-eventstore/output/sample.csv')



  // do something with the dataframe here!

})

