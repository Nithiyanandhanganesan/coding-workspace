package com.wrs.datamanagement.dao.bigtable;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.wrs.datamanagement.model.IdData;
import com.wrs.datamanagement.util.BigTableConnector;
import com.wrs.datamanagement.util.DateUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CustomerToPartyIdDao {

    private static final Logger logger = LoggerFactory.getLogger(CustomerToPartyIdDao.class);
    public static final byte[] bCustomer = Bytes.toBytes("customer");
    public static final byte[] bPartyId = Bytes.toBytes("party_id");
    public static final byte[] bPartyOwner = Bytes.toBytes("party_owner");
    public static final byte[] bCreatedDate = Bytes.toBytes("created_date");
    public static final byte[] bCid = Bytes.toBytes("cid");
    public static final byte[] bUpdatedDate = Bytes.toBytes("updated_date");

    private BigTableConnector bigTableConnector;

    public static final String tableName = "customerid_to_partyid";
    public static final String columnFamily1 = "i";
    public static final String columnFamily2 = "u";

    private byte[] bytesTable = Bytes.toBytes(tableName);
    private byte[] bytesColumnFamily1 = Bytes.toBytes(columnFamily1);
    private byte[] bytesColumnFamily2 = Bytes.toBytes(columnFamily2);
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();



    public CustomerToPartyIdDao(BigTableConnector bigTableConnector) {
        this.bigTableConnector = bigTableConnector;
    }

    public String save(String customer, String partyOwner) {

        String partyId = null;
        Table table = null;
        try {
            table = bigTableConnector.getBigTableConnection().getTable(TableName.valueOf(tableName));
            String rowKey = customer + "#" + partyOwner;
            partyId = UUID.nameUUIDFromBytes(customer.getBytes()).toString();
            String creationDate = DateUtil.formatDateWithString(null);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(bytesColumnFamily1, bCustomer, Bytes.toBytes(customer));
            put.addColumn(bytesColumnFamily1, bPartyId, Bytes.toBytes(partyId));
            put.addColumn(bytesColumnFamily1, bPartyOwner, Bytes.toBytes(partyOwner));
            put.addColumn(bytesColumnFamily1, bCreatedDate, Bytes.toBytes(creationDate));
            table.put(put);
            return partyId;
        } catch (IOException e) {
            logger.error("Exception occured while inserting partyid for appnexus {}, {}",customer, partyOwner, e);
        }
        finally
        {
            if(table != null)
            {
                try
                {
                    table.close();
                } catch (IOException e) {
                    logger.error("Exception occured closing table {}", table.toString(), e);
                }
            }
        }
        return partyId;
    }

    public List<String> saveBatch(List<IdData> objs)
    {
        ArrayList<String> partyids = new ArrayList<>(objs.size());
        Table table = null;
        try {
            table = bigTableConnector.getBigTableConnection().getTable(TableName.valueOf(tableName));
            List<Put> rows = new ArrayList<>(objs.size());
            for(IdData obj: objs) {
                String rowKey = obj.getId() + "#" + obj.getPartyOwner();
                String partyId = obj.getPartyId();
                partyids.add(partyId);
                String creationDate = DateUtil.formatDateWithString(null);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(bytesColumnFamily1, bCustomer, Bytes.toBytes(obj.getId()));
                put.addColumn(bytesColumnFamily1, bPartyId, Bytes.toBytes(partyId));
                put.addColumn(bytesColumnFamily1, bPartyOwner, Bytes.toBytes(obj.getPartyOwner()));
                put.addColumn(bytesColumnFamily1, bCreatedDate, Bytes.toBytes(creationDate));
                rows.add(put);
            }
            table.put(rows);
            return partyids;
        } catch (IOException e) {
            logger.error("Exception occured while inserting partyids ",  e);
        }
        finally
        {
            if(table != null)
            {
                try
                {
                    table.close();
                } catch (IOException e) {
                    logger.error("Exception occured closing table {}", table.toString(), e);
                }
            }
        }
        return partyids;
    }

    public void delete(String customer, String partyOwner) {

        Table table;
        try {
            table = bigTableConnector.getBigTableConnection().getTable(TableName.valueOf(tableName));
            //log.info("Deleting row");
            String rowKey = customer + "#" + partyOwner;
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String findPartyIdByRowKey(String customer, String partyOwner){

        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter rowFilter = new PrefixFilter(Bytes.toBytes(customer));

        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
                bytesColumnFamily1, bPartyOwner, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(partyOwner));

        list.addFilter(rowFilter);
        return query(list);
    }


    private String query(FilterList list) {
        String partyId = null;
        Scan scan = new Scan();
        Table table;
        try {
            table = bigTableConnector.getBigTableConnection().getTable(TableName.valueOf(tableName));
            scan.setFilter(list);
            ResultScanner scanner = table.getScanner(scan);
            for (Result row : scanner) {
                byte[] bpartyId = row.getValue(bytesColumnFamily1, bPartyId);

                if (bpartyId != null && bpartyId.length > 0) {
                    return Bytes.toString(bpartyId);
                }
            }
            scanner.close();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return partyId;
    }
}
