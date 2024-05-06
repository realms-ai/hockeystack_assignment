const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');
const util = require('util')
const fs = require('fs')
const path = require('path');
const debugLog = util.debuglog('curl');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ acfscessToken: null});
const propertyPrefix = 'hubspot__';
let expirationDate;
// let expirationDate = new Date(Date.now() + (86500*1000));

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  // if(new Date() < expirationDate) return
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;
  const rt = process.env.HUBSPOT_REFRESH_TOKEN
  debugLog("\n\n\n\n\nTesting Debug Logging\n\n\n\n\n")
  return hubspotClient.oauth.tokensApi
    .create('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, rt || refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());
      console.info("Access Token: ", newAccessToken)

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

// Finding All the Properties in the Object and evaluate which are needed in a request for optimal data. 
const getObjectProperties = async (objectType = 'contacts', fileName='contactProperties.json') => {
  // First Read Data from the File, if null then populate it with data
  const file = path.join(__dirname, 'data', fileName)
  let data = await fs.readFileSync(file, { encoding: 'utf8', flag: 'r' })
  data = JSON.parse(data)
  console.info(`${objectType} properties: `)
  if(data.length > 0){
    console.info("Local Data: ", data);
    return data
  } else {
    const archived = false;
    const properties = undefined;
  
    try {
      const apiResponse = await hubspotClient.crm.properties.coreApi.getAll(objectType, archived, properties);
      // console.log(JSON.stringify(apiResponse, null, 2));
  
      const refinedResponse = apiResponse?.results.map(e => {
        return {
          name: e.name,
          description: e.description
        }
      })
      console.info("Refined Response: ", util.inspect(refinedResponse, false, 3, true));
      // debugger
      fs.writeFile(file, JSON.stringify(refinedResponse), (err) => {
        // In case of a error throw err.
        if (err) throw err;
      })
      return refinedResponse
    } catch (e) {
      e.message === 'HTTP request failed'
        ? console.error(JSON.stringify(e.response, null, 2))
        : console.error(e)
    }
  }
}

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount = 4) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};
  const limit = 100;

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    console.info("Last Modified Date Filter: ", lastModifiedDateFilter)
    const searchObject = {
      // filterGroups: [lastModifiedDateFilter],
      sorts: [{ propertyName: 'lastmodifieddate', direction: 'ASCENDING' }],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount = 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    console.info("Search Result Contacts: ", util.inspect(searchResult, false, 3, true) )

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { inputs: contactsToAssociate.map(contactId => ({ id: contactId })) }
    })).json())?.results || [];

    console.info("Company Associates: ", companyAssociationsResults)

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);

  return true;
};

// Get Meetings

const getMeetings = async(lastPulledDate, now, offsetObject) => {
  const limit = 10;

  const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
  const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
  // Finding Meetings which actually happened with attended 
  const finalFilter = {
    filters: [
      ...lastModifiedDateFilter.filters, 
      // {
      //   propertyName: 'hs_meeting_outcome',
      //   operator: 'IN',
      //   values: ['COMPLETED', 'SCHEDULED']
      // }
    ]
  }
  console.info("Final Filter: ", finalFilter)
  // let meetingProperties = await getObjectProperties('meetings', 'meetingProperties.json')
  // debugger
  const searchObject = {
    filterGroups: [finalFilter],
    sorts: [{ propertyName: 'hs_timestamp', direction: 'ASCENDING' }],
    properties: //meetingProperties.map(e => e.name),
    [
      'hs_timestamp',
      'hs_meeting_title',
      'hubspot_owner_id',
      'hubspot_owner_id',
      'hs_meeting_start_time',
      'hs_meeting_end_time',
      'hs_createdate',
      'hs_user_ids_of_all_owners',
      'hs_user_ids_of_all_notification_unfollowers',
      'hs_user_ids_of_all_notification_followers',
      'hs_unique_id',
      'hs_all_owner_ids',
      'hs_guest_emails',
      "hs_outcome_canceled_count",
      "hs_outcome_completed_count",
      "hs_outcome_no_show_count",
      "hs_outcome_rescheduled_count",
      "hs_outcome_scheduled_count",
      'hs_meeting_outcome',  // ['COMPLETED', 'SCHEDULED', 'RE-SCHEDULED', 'CANCELLED', 'NO_SHOW', 'CANCELLED']
      'hs_attendee_owner_ids',
      'hs_product_name'
    ],
    limit: limit,
    after: offsetObject.after,
    // associations: ['contacts', 'companies'] // Don't work in Search (Only in Basic) 
  };

  let searchResult = {};

  // TASK 1: Fetch Meetings with Search API from the HUBSPOT
  while (tryCount = 4) {
    try {
      const request = hubspotClient.crm.objects.meetings.searchApi.doSearch(searchObject)    
      searchResult = await request;
      break;
    } catch (err) {
      tryCount++;

      if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

      await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
    }
  }

  console.info("Search Result Meetings: ", util.inspect(searchResult, false, 3, true))

  if (!searchResult) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');

  console.log('fetch Meeting batch');

  offsetObject.after = parseInt(searchResult.paging?.next?.after);

  return searchResult.results || [];
}

// Get ContactAssociations
const getAssociation = async (data) => {
  // Task 3: Finding which contacts attended the meeting
  // Way 1: Using Engagments API https://developers.hubspot.com/beta-docs/reference/api/crm/engagements/engagement-details
  // const gettingAllEngagements = await hubspotClient.apiRequest({
  //   path: 'engagements/v1/engagements/paged?limit=2'
  // }) // .json())?.results || [];

  // Way 2: Using Meetings Basic Read API: https://developers.hubspot.com/beta-docs/reference/api/crm/engagements/meetings#get-%2Fcrm%2Fv3%2Fobjects%2Fmeetings%2F%7Bmeetingid%7D

  // Way 3: Using Associations API
  const meetingIds = data.map(meeting => meeting.id);

  // meeting to contact association
  const contactAssociationsResults = (await (await hubspotClient.apiRequest({
    method: 'post',
    path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
    body: { inputs: meetingIds.map(meetingId => ({ id: meetingId })) }
  })).json())?.results || [];

  console.info("Contact Associates: ", contactAssociationsResults)

  let contactIds = []
  const contactAssociations = Object.fromEntries(contactAssociationsResults.map(a => {
    if (a.from) {
      const cIds = a.to.map(e => {
        contactIds.push(e.id)
        return e.id
      })
      return [a.from.id, cIds];
    } else return false;
  }).filter(x => x));

  console.info("Contact Meeting Association: ", contactAssociations)

  // Get unique contacts Ids
  contactIds = contactIds.filter((v,i,self) => i == self.indexOf(v))
  const contacts = await getContactsData(contactIds)
  
  Object.entries(contactAssociations).forEach(([k,v]) => 
    contactAssociations[k] = v.map(contactId => contacts[contactId])
  )
  return contactAssociations
}

// Get Contacts
const getContactsData = async (contactIds) => {
  // Get Data of contacts which contains their emails and name
  const contactObject = {
    propertiesWithHistory: null,
    inputs: contactIds,
    properties: [
      'firstname',
      'lastname',
      'jobtitle',
      'email',
    ]
  }
  const request = hubspotClient.apiRequest({
    method: 'post',
    path: '/crm/v3/objects/contacts/batch/read?archived=false',
    body: contactObject
  })

  let contacts = (await request).json()?.results || [];
  
  return Object.fromEntries(contacts.map(contact => {
    return [contact.id, contact.properties]
  }).filter(x => x))
  console.info("Contacts Data: ", util.inspect(contacts, false, 5, true))
}


// Get Meetings & Clean Data
const processMeetings = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  // Use below startDate to fetch older data which is before the lastPulledDates
  // const lastPulledDate = new Date(Date.parse(account.lastPulledDates.meetings) - (86400*365*2*1000));
  const lastPulledDate = new Date(Date.parse(account.lastPulledDates.meetings));
  console.info("Last pulled Meeting Date: ", lastPulledDate)
  const now = new Date();
  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const data = await getMeetings(lastPulledDate, now, offsetObject)
    const contactAssociations = await getAssociation(data)
    data.forEach(async(meeting) => {
      if (!meeting.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          meeting_id: meeting.id,
          meeting_outcome: meeting.properties.hs_meeting_outcome,
          meeting_title: meeting.properties.title,
          meeting_product: meeting.properties.hs_product_name,
          meeting_created: meeting.createdAt,
          contacts: contactAssociations[meeting.id]
        }
      };

      console.info("Meeting Action Template: ", actionTemplate)

      // TASK 2: Insert Actions **Meeting Created** and **Meeting Completed**
      const isCreated = new Date(meeting.createdAt) > lastPulledDate
      const isCompleted = new Date(meeting.properties.hs_timestamp) > lastPulledDate && meeting.properties.hs_meeting_outcome === 'COMPLETED'
      
      const actionsToPush = []
      if(isCreated) {
        actionsToPush.push({
          actionName: 'Meeting Created',
          actionDate: new Date(meeting.createdAt) - 2000,
          ...actionTemplate
        })
      }
      if(isCompleted) {
        actionsToPush.push({
          actionName: 'Meeting Completed',
          actionDate: new Date(meeting.updatedAt) - 2000,
          ...actionTemplate
        })
      }

      actionsToPush.map(action => q.push(action))
    })

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.meetings = now;
  await saveDomain(domain);

  return true
}



const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});
  // Domain Data
  /*
    Domain:  {
      company: { name: 'Test Account', website: 'test.com' },
      integrations: { hubspot: { status: true, accounts: [Array] } },
      _id: new ObjectId("5f667c45c60ee353ad89d5d3"),
      customer: new ObjectId("5f2136c2a5c69924e747ba91"),
      setup: true,
      apiKey: '92ee9e4a8b15fc058046998d5e9fbe',
      customerDBName: '92ee9e4a8b15fc058046998d5e9fbe',
      blacklist: [],
      __v: 833,
      sharing: { enabled: false, public: true },
      blacklistUUID: [],
      logo: 'https://i.ibb.co/M6XmKH8/hockeystack-logo-circle.png',
      connectedDomains: [],
      dontCollect: false,
      surveysSetup: true,
      overMonthlyLimit: false,
      mailPreferences: { weeklyReport: false },
      currencies: [ 'USD', 'TRY' ],
      mainCurrency: 'USD',
      customers: [
        {
          mailPreferences: [Object],
          accessLevel: 'creator',
          _id: new ObjectId("620e5d15770130cc7bc7773e"),
          customerId: new ObjectId("5f2136c2a5c69924e747ba91")
        }
      ],
      trialEndDate: 2020-08-07T08:43:46.742Z,
      trialStartDate: 2020-07-29T08:43:46.742Z,
      turnOffThirdPartyScripts: false,
      privacyMode: false,
      finishedOnboarding: true,
      forceMfa: false,
      loginConnection: { loginType: 'password' }
    }
  */
  console.info("Domain: ", util.inspect(domain, false, 5, true ))

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');
    console.info("Account: ", account)
    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);
    console.info("Q: ", q)
    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log("Error: ", err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processCompanies', hubId: account.hubId } });
    }

    // Get Meetings
    try {
      await processMeetings(domain, account.hubId, q);
      console.log('process meetings');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processMeetings', hubId: account.hubId } });
    }

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
