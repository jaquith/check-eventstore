// Declare global variables for Standard JS (linter)
/* global describe, it */
'use strict'

const chai = require('chai')
const fse = require('fs-extra')
const path = require('path')

const _ = require('lodash')

const ocmProfiles = JSON.parse(fse.readFileSync(path.join(__dirname, '../output/ocm-profiles.json'), 'utf8'))
let eventsCopy

const consentAttributes = [
  "42ads Consent",
  "8SELECT Consent",
  "AWIN Consent",
  "Bing_Ads Consent",
  "Criteo Consent",
  "Emarsys Consent",
  "Facebook_Connect Consent",
  "Facebook_Pixel Consent",
  "Fitanalytics Consent",
  "Google_Ads Consent",
  "Google_OAuth Consent",
  "HI_Share_that Consent",
  "Instagram_Content Consent",
  "Outbrain Consent",
  "Sovendus Consent",
  "Tealium_Inc Consent",
  "YouTube_Video Consent",
  "epoq Consent"
]

const isConsentEvent = function (event) {
  let udoData = event && event.data && event.data.udo
  return udoData && (udoData.tealium_event === "consents_initialized" || udoData.tealium_event === "consents_changed_finished" || udoData.tealium_event === "consent_changed_tealium_false")
}

const compareConsentSettings = function (profile, lastConsentEvent) {
  consentAttributes.forEach(function (attr) {
    let eventLevelName = attr.split(' Consent')[0]
    let eventLevelConsent = lastConsentEvent.data.udo[eventLevelName]

    // there seem to be event-level capitalizations for YouTube/Youtube
    if (eventLevelName === 'YouTube_Video') {
      eventLevelConsent = typeof lastConsentEvent.data.udo[eventLevelName] === 'boolean' ? lastConsentEvent.data.udo[eventLevelName] : lastConsentEvent.data.udo['Youtube_Video']
    }
    let profileLevelConsent = profile.flags[attr]
    chai.expect(eventLevelConsent, `${eventLevelName}`).to.be.a('boolean')
    chai.expect(profileLevelConsent, `${attr}`).to.be.a('boolean')

    chai.expect(eventLevelConsent, `${eventLevelName}`).to.equal(profile.flags[attr])



  })
}

describe('the outletcity/main profiles from AudienceStore that did a consent_changed_tealium_false event', function () {
  ocmProfiles.forEach(function (profile) {
    describe(`profile ${profile._id} `, function () {
      it('should have an "_id" attribute', function () {
        chai.expect(profile._id).to.be.a('string')
      })
      
      it('should have an "events" array of the current visit\'s events', function () {
        eventsCopy = JSON.parse(JSON.stringify(profile.current_visit.events))
        chai.expect(profile.current_visit.events).to.be.an('array').with.length.greaterThan(0)
      })

      it('each "event" should have a timestamp', function () {
        eventsCopy.forEach(function (event) {
          chai.expect(event.post_time).to.be.a('number')
        })
      })

      it('the "events" should be in chronological order', function () {
        eventsCopy = _.sortBy(eventsCopy, ['post_time'])
        eventsCopy.forEach(function (event, i, arr) {
          let eventTime = event.post_time
          let lastPostTime = i > 0 ? arr[i-1].post_time : 0
          chai.expect(lastPostTime).to.be.lessThanOrEqual(eventTime)
        })
      })

      it('the profile should have all the consent attributes', function () {
        consentAttributes.forEach(function (attr) {
          chai.expect(profile.flags[attr]).to.be.a('boolean')
        })
      })

      it('if "events" has at least one consent event, the profile flags should match the latest decision', function () {
        let counter = 0
        let lastConsentEvent
        eventsCopy.forEach(function (event) {
          if (isConsentEvent(event)) {
            counter++
            lastConsentEvent = event
          }
        })
        if (counter > 0) {
          compareConsentSettings(profile, lastConsentEvent)
          /*
          try {
            compareConsentSettings(profile, lastConsentEvent)
          } catch (e) {
            console.log(e)
          }
          */
          
        }
      })
    })
  })
})


