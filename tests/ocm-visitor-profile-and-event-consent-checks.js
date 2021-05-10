// Declare global variables for Standard JS (linter)
/* global describe, it */
'use strict'

const chai = require('chai')
const fse = require('fs-extra')
const path = require('path')

const _ = require('lodash')

const ocmSummary = JSON.parse(fse.readFileSync(path.join(__dirname, '../output/ocm-summary.json'), 'utf8'))


let profiles, anonymousIds
describe('the outletcity/main summary', function () {
  describe('the summary structure', function () {
    it('should have the expected structure', function () {
      chai.expect(ocmSummary).to.be.an('object').with.property('customerIds').that.is.an('object')
      chai.expect(ocmSummary).to.be.an('object').with.property('anonymousIds').that.is.an('object')
    })
  })
 
  Object.keys(ocmSummary.customerIds).forEach(function (customerId) {
    describe(`profile ${customerId} `, function () {
      it(`the profile (${customerId}) should have at least one anonymousId and one timestamped profile`, function () {
        const thisEntry = ocmSummary.customerIds[customerId]
        chai.expect(thisEntry).to.be.an('object').with.property('anonymousIds').that.is.an('object')
        chai.expect(thisEntry).to.be.an('object').with.property('profiles').that.is.an('object')
        profiles = Object.keys(thisEntry.profiles)
        anonymousIds = Object.keys(thisEntry.anonymousIds)
        chai.expect(profiles).to.be.an('array').with.length.greaterThan(0)
        chai.expect(anonymousIds).to.be.an('array').with.length.greaterThan(0)
      })
    })
  })

})



