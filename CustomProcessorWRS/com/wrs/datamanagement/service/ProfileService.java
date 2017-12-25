package com.wrs.datamanagement.service;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor.Context;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.SingleLaneProcessor.SingleLaneBatchMaker;
import com.wrs.datamanagement.dao.bigtable.*;
import com.wrs.datamanagement.model.IdData;
import com.wrs.datamanagement.util.BigTableConnector;
import com.wrs.datamanagement.util.PartyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

public class ProfileService {
  private static final Logger logger = LoggerFactory.getLogger(EventService.class);
  private final Context context;
  private final String customerIdField;
  private final String customerIdType;
  private final CustomerToPartyIdDao customerToPartyIdDao;
  private final CssToPartyIdDao cssToPartyIdDao;
  private final AppNexusToPartyIdDao appNexusToPartyIdDao;
  private final OsvendoridToPartyIdDao osvendoridToPartyIdDao;
  private final CredentialToPartyIdDao credentialToPartyIdDao;
  private final String partyOwnerField;
  private BigTableConnector bigTableConnector;

  public ProfileService(Context context, String projectId, String instanceId, String customerIdField, String customerIdType, String partyOwnerField) {
    this.context = context;
    this.bigTableConnector = BigTableConnector.getInstance(projectId, instanceId);
    this.customerIdField = customerIdField;
    this.customerIdType = customerIdType;
    this.partyOwnerField = partyOwnerField;

    this.appNexusToPartyIdDao = new AppNexusToPartyIdDao(this.bigTableConnector);
    this.cssToPartyIdDao = new CssToPartyIdDao(this.bigTableConnector);
    this.customerToPartyIdDao = new CustomerToPartyIdDao(this.bigTableConnector);
    this.osvendoridToPartyIdDao = new OsvendoridToPartyIdDao(this.bigTableConnector);

    this.credentialToPartyIdDao = new CredentialToPartyIdDao(this.bigTableConnector);
  }

  public void transform(Batch batch, SingleLaneBatchMaker batchMaker) {
    ArrayList<IdData> idDatalist = new ArrayList<>();
    for (Iterator<Record> it = batch.getRecords(); it.hasNext(); ) {
      Record record = it.next();
      String partyId = PartyUtil.getDataPartyId(record.get(customerIdField).getValueAsString());
      String id = record.get(customerIdField).getValueAsString();
      String partyOwner = record.get(partyOwnerField).getValueAsString();

      switch (this.customerIdType) {
        case "EMAIL":
        case "MOBILE_NUMBER":
        case "PSID":
          this.credentialToPartyIdDao.save(id, partyOwner, this.customerIdType, null);
        default:
          break;
      }

      record.set("/party_id", Field.create(partyId));
      batchMaker.addRecord(record);

      idDatalist.add(new IdData(partyId, id, partyOwner));
    }

    // Commiting to database
    try {
      this.credentialToPartyIdDao.commit();
    } catch (Exception e) {
      for (Iterator<Record> it = batch.getRecords(); it.hasNext(); ) {
        context.toError(it.next(), e);
      }
    }

    // Legacy stuff would like to change to commit api
    switch (this.customerIdType) {
      case "APPNEXUS":
        this.appNexusToPartyIdDao.saveBatch(idDatalist);
        break;
      case "CSS":
        this.cssToPartyIdDao.saveBatch(idDatalist);
        break;
      case "CUSTOMER":
        this.customerToPartyIdDao.saveBatch(idDatalist);
        break;
      case "OSVENDOR":
        this.osvendoridToPartyIdDao.saveBatch(idDatalist);
        break;
      default:
        break;
    }

  }

}