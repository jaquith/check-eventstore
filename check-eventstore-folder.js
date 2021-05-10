#!/usr/bin/env node



/**
 * This has been written specifically to verify outletcity/main
 * 
 * 1. Get a finite (configured) number of profiles with 'consent_changed_tealium_false' events by going through EventStore, only as far as we need
 * 2. Go though the EventStore files once to get all anonymousIds associated with those profiles
 * 3. Go though the EventStore files AGAIN to get all the events associated with those anonymousIds
 * 4. Go though the AudienceStore files to get all the profiles
 * 
 * After that, there are some summary/output files that we can run assertions on
 */

// reference: https://medium.freecodecamp.org/writing-command-line-applications-in-nodejs-2cf8327eee2
'use strict';

const fs = require('fs-extra')
const zlib = require('zlib')

const profilesToCheck = 20

const DataFrame = require('dataframe-js').DataFrame;

// folder to pull .gz files from
const eventStoreFolder = '../../Desktop/ocm/store/events';
const audienceStoreFolder = '../../Desktop/ocm/store/profiles';

const attributeIdCustomerId = '5276'

const startTime = Date.now()

const announceStep = function (stepNumber) {

}


const customerIds = getCustomerIdsFromEventStore(eventStoreFolder, profilesToCheck)
console.log(customerIds)
let output = {}
output.customerIds = {}
output.anonymousIds = {}
output = getAllEventsForAUserList(eventStoreFolder, customerIds, output)
output = runAudienceStoreLogic(customerIds, audienceStoreFolder, output)
fs.writeJSONSync('/Users/calebjaquith/.git/check-eventstore/output/ocm-summary.json', output, {spaces: 2})

// FUNCTIONS

function isGzip(file) {
  return /\.gz$/.test(file);
}

function getCustomerIdsFromEventStore (eventStoreFolder, max) {
  const files = fs.readdirSync(eventStoreFolder)
  let customerIds = {}
  console.log('\n')
  files.forEach((file) => {
    if (Object.keys(customerIds).length || !isGzip(file)) return

    const path = `${eventStoreFolder}/${file}`
    if (!isGzip(file)) return 
    console.log("--- STEP 1/4 - FETCHING QUALIFYING IDs from " + path + " ----" )
    // this should be OK because the files are relatively small by design in EventStore, and it makes the code simpler
    let data = fs.readFileSync(path);
    var treeStream = Buffer.from(data);
    let buffer = zlib.unzipSync(treeStream);
    var string = buffer.toString();
    let events = string.split("\n").map(el => JSON.parse(el))

    events.forEach((event) => {
      if (Object.keys(customerIds).length > max - 1) return
      // const isConsentEvent = event.udo_tealium_event === "consents_initialized" || event.udo_tealium_event === "consents_changed_finished" || event.udo_tealium_event === "consent_changed_tealium_false"
      if (event.udo_tealium_event === "consent_changed_tealium_false" && event.udo_customer_id) {
        customerIds[event.udo_customer_id] = true
      }
    })
  })
  return Object.keys(customerIds)
}

function getAllEventsForAUserList(eventStoreFolder, attributeValueList, output) {
  
  attributeValueList.forEach((secondaryId) => {
    output.customerIds[secondaryId] = output.customerIds[secondaryId] || {}
    output.customerIds[secondaryId].anonymousIds = output.customerIds[secondaryId].anonymousIds || {}
  })

  const files = fs.readdirSync(eventStoreFolder)

  const fileCount = files.length
  console.log('\n')
  files.forEach((file, i, arr) => {
    // just for testing
    // if (i > 1) return
    if (!isGzip(file)) return 

    const path = `${eventStoreFolder}/${file}`

    console.log(`--- STEP 2/4 - READING EVENTSTORE FILE ${i+1}/${fileCount} - ` + path + " ----" )
    // this is OK because files are relatively small by design in EventStore, and it makes the code simpler if we can do it sync
    const data = fs.readFileSync(path);
    const treeStream = Buffer.from(data);
    const buffer = zlib.unzipSync(treeStream);
    const string = buffer.toString();
    const events = string.split("\n").map(el => JSON.parse(el))

    // get the anonymous IDs
    events.forEach((event) => {
      const eventCustomerId = getCustomerId(event)
      if (eventCustomerId && customerIds.indexOf(eventCustomerId) !== -1) {
        output.customerIds = output.customerIds || {}
        output.customerIds[eventCustomerId] = output.customerIds[eventCustomerId] || {}
        output.customerIds[eventCustomerId].anonymousIds = output.customerIds[eventCustomerId].anonymousIds || {}

        const anonymousId = event.udo_tealium_visitor_id || event.visitorid

        output.customerIds[eventCustomerId].anonymousIds[anonymousId] = output.customerIds[eventCustomerId].anonymousIds[anonymousId] || 0
        output.customerIds[eventCustomerId].anonymousIds[anonymousId]++

        output.anonymousIds[anonymousId] = output.anonymousIds[anonymousId] || {}

        output.anonymousIds[anonymousId].customerIds = output.anonymousIds[anonymousId].customerIds || {}
        output.anonymousIds[anonymousId].customerIds[eventCustomerId] = output.anonymousIds[anonymousId].customerIds[eventCustomerId] || 0
        output.anonymousIds[anonymousId].customerIds[eventCustomerId]++
      }
    })
  })

  // fs.writeJSONSync('/Users/calebjaquith/.git/check-eventstore/output/ocm-summary-first-pass.json', output, {spaces: 2})
  console.log('\n')
  files.forEach((file, i, arr) => {
    // just for testing
    // if (i > 1) return
    if (!isGzip(file)) return 

    const path = `${eventStoreFolder}/${file}`

    console.log(`--- STEP 3/4 - READING EVENTSTORE FILE ${i+1}/${fileCount} - ` + path + " ----" )
    // this is OK because files are relatively small by design in EventStore, and it makes the code simpler if we can do it sync
    const data = fs.readFileSync(path);
    const treeStream = Buffer.from(data);
    const buffer = zlib.unzipSync(treeStream);
    const string = buffer.toString();
    const events = string.split("\n").map(el => JSON.parse(el))

    // get all the events for those anonymous ids
    events.forEach((event) => {
      const allAnonymousIds = Object.keys(output.anonymousIds)
      const anonymousId = event.udo_tealium_visitor_id || event.visitorid
      const isRelevant = allAnonymousIds.indexOf(anonymousId) !== -1
      if (isRelevant) {
        
        output.anonymousIds[anonymousId].events = output.anonymousIds[anonymousId].events || []
        output.anonymousIds[anonymousId].events.push(event)
      }
    });
  })

  // fs.writeJSONSync('/Users/calebjaquith/.git/check-eventstore/output/ocm-summary-second-pass.json', output, {spaces: 2})
  return output
}

function runAudienceStoreLogic (customerIds, audienceStoreFolder, output) {
  const files = fs.readdirSync(audienceStoreFolder)
  let targetProfiles = []
  let keyCollector = {}

  let fileCount = files.length
  console.log('\n')
  files.forEach((file, i, arr) => {
    if (!isGzip(file)) return

    const path = `${audienceStoreFolder}/${file}`

    console.log(`--- STEP 4/4 - READING AUDIENCESTORE FILE ${i+1}/${fileCount} - ` + path + " ----" )
    // this is OK because files are relatively small by design in *Store, and it makes the code simpler if we can do it sync
    let data = fs.readFileSync(path);
    var treeStream = Buffer.from(data);
    let buffer = zlib.unzipSync(treeStream);
    var string = buffer.toString();
    let profiles = string.split("\n").map(el => JSON.parse(el))

    profiles.forEach((profile) => {
      const customerId = profile && profile.secondary_ids && profile.secondary_ids[attributeIdCustomerId]
      const isTargetProfile = customerId && customerIds.indexOf(customerId) !== -1
      if (isTargetProfile) {
        targetProfiles.push(profile)
        output.customerIds[customerId] = output.customerIds[customerId] || {}
        output.customerIds[customerId].profiles = output.customerIds[customerId].profiles || {}
        output.customerIds[customerId].profiles[profile.metrics.ts_in_ms] = profile
      }

      let attributeGroups = Object.keys(profile)
      attributeGroups.forEach((key) => {
        if (!key) return
        keyCollector[key] = true
      })
    })
    // do something with the dataframe here!
  })

  const df = new DataFrame(targetProfiles, Object.keys(keyCollector));
    
  df.show(20, false);
  fs.writeJsonSync('/Users/calebjaquith/.git/check-eventstore/output/ocm-profiles.json', targetProfiles, {spaces:2})
  console.log('WROTE output/ocm-profiles.json')

  return output
  
}

function getCustomerId (event) {
  return event.udo_recommendation_customerdata_customerid || event.udo_customer_id || (event.tealium_event === "hybris_registration" || event.tealium_event === "hybris_email_address_change") && event.tealium_visitor_id
}


