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
    });
  });

  console.log(allRecords.length)
  
  const keyCollector = {}
  allRecords.forEach((event) => {
    let eventKeys = Object.keys(event)
    eventKeys.forEach((eventKey) => {
      if (!eventKey) return
      keyCollector[eventKey] = true
    })
  })

  let allKeys = Object.keys(keyCollector)

  let myKeys = [
    'pageurl_full_url',
    'dom_url',
    'udo_tealium_event',
    'dom_query_string',
    'eventid'
  ]

  const df = new DataFrame(allRecords, myKeys);

  df.show();

})

