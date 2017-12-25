package com.wrs.datamanagement.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.wrs.datamanagement.exception.WRSDataException;
import com.wrs.datamanagement.lib.errors.Errors;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bmangalam on 4/19/17.
 */
public class EventService {
    private static final Logger logger = LoggerFactory.getLogger(EventService.class);

    //private static final String ATTRIBUTE_QUERY = "select ds_src, atr_status, datasource_id, ds_entity, raw_atr, taxon_atr, is_null, behavior_map, is_masked, is_encrypted, info_sec_class, is_global  FROM wrsdata.ds_taxonomy where datasource_id = ? and  atr_status =1 and ds_entity=?;";

    private ConcurrentHashMap<MultiKey, Map<String, String>> eventMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Map<String, Map<String, String>>> attrMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<MultiKey, Map<String, String>> identifierMap = new ConcurrentHashMap<>();
    //private List<String> evtColumns = Arrays.asList(new String[]{"datasource_id","event_def_id","status", "ds_entity", "evt_map_logic, event_definition"});
    //private List<String> attrColumns = Arrays.asList(new String[]{"ds_src","atr_status","datasource_id","raw_atr","behavior_map","ds_entity","taxon_atr","info_sec_class","is_null","is_encrypted","is_global","is_masked"});
    private List<String> evtColumns = null;
    private List<String> attrColumns = null;
    private static final String WHAT = "What";
    private static final String WHERE = "Where";
    private static final String WHY = "Why";
    private static final String WHEN = "When";
    private static final String WHO = "Who";
    private static final String HOW = "How";
    private static final String EVENT = "/event";
    private static final String DATASOURCE_ID = "/datasource_id";
    private static final String EVENT_ID = "/event_id";
    private static final String DATE_TIME = "/datetime";
    private static final String PARTY_ID = "/party_id";
    private static final String PARTY_OWNER = "/party_owner";
    private static final String ROW_KEY = "/row_key";
    private static final String FLATTENED_EVENT = "/flattened_event";
    private static final String EVENT_DEF_ID = "event_def_id";
    private static final String EVENT_DEFINITION = "event_definition";
    private static final List<String> BEHAVIOR_MAP = Arrays.asList("who", "what", "when", "where", "why", "how");
    private static final String OUT_FORMAT = "yyyyMMddHHmmssSSS";
    private String eventQuery = null;
    private String attrQuery = null;
    private String attrColumnQuery = null;
    private String evtTsAttribute = null;
    private String inputFormat = null;
    private HikariDataSource hds = null;

    private DateTimeFormatter iFormatter = null;
    private DateTimeFormatter oFormatter = DateTimeFormatter.ofPattern(OUT_FORMAT);

    Gson gson = null;
    String datasourceId = null;

    public EventService(String eQuery, String aQuery, String cQuery, String dateTimeFormat, String evtTsAttr, HikariDataSource ds) {
        eventQuery = eQuery;
        attrQuery = aQuery;
        attrColumnQuery = cQuery;
        evtTsAttribute = evtTsAttr;
        inputFormat = dateTimeFormat;
        iFormatter = DateTimeFormatter.ofPattern(inputFormat);
        gson = new Gson();
        hds = ds;
        try {
            populateTaxonomy();
        }
        finally{
            if(hds != null) {
                IOUtils.closeQuietly(hds);
            }
            hds = null;
        }
    }

    public Record transform(Record record) throws StageException
    {
        ConcurrentHashMap<String, Field> fieldMap = new ConcurrentHashMap<>(record.get().getValueAsMap());
        MultiKey multiKey = this.identifyEvent(fieldMap);
        Field wField = record.get(PARTY_ID);
        String partyId = wField != null ? wField.getValueAsString() : "";
        Field ownerField = record.get(PARTY_OWNER);
        String partyOwner = ownerField != null ? ownerField.getValueAsString() : "";
        String dateTime = null;


        //Add default attributes
        fieldMap.put("party_id", Field.create(partyId));
        fieldMap.put("party_owner", Field.create(partyOwner));

        try {

            if (multiKey != null) {
                String eventId = (String)multiKey.getKey(0);
                logger.debug("Event=" + eventId);
                fieldMap.put(EVENT_DEFINITION, Field.create(eventMap.get(multiKey).get(EVENT_DEFINITION)));
                fieldMap.put(EVENT_DEF_ID, Field.create(eventId));
                Map<String, Map<String, String>> map = attrMap.get(eventId);
                if (map != null && map.size() > 0) {
                    JsonObject event = new JsonObject();
                    JsonObject who = new JsonObject();
                    JsonObject what = new JsonObject();
                    JsonObject where = new JsonObject();
                    JsonObject why = new JsonObject();
                    JsonObject when = new JsonObject();
                    JsonObject how = new JsonObject();
                    JsonObject flattenedEvent = new JsonObject();
                    for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
                        String key = entry.getKey();
                        Field field = entry.getValue();
                        Map<String, String> attrDefs = map.get(key);
                        if (attrDefs != null) {
                            String behavior = attrDefs.get("behavior_map");
                            logger.debug("Behavior=" + behavior + " key=" + key);
                            if (BEHAVIOR_MAP.contains(attrDefs.get("behavior_map"))) {
                                String value = field.getValueAsString();
                                value = (value == null) ? "" : value;
                                flattenedEvent.addProperty(attrDefs.get("taxon_atr"), value);
                                switch (behavior) {
                                    case "why":
                                        why.addProperty(attrDefs.get("taxon_atr"), value);
                                        break;
                                    case "where":
                                        where.addProperty(attrDefs.get("taxon_atr"), value);
                                        break;
                                    case "what":
                                        what.addProperty(attrDefs.get("taxon_atr"), value);
                                        break;
                                    case "when":
                                        when.addProperty(attrDefs.get("taxon_atr"), value);
                                        if (evtTsAttribute.equals(attrDefs.get("taxon_atr"))) {
                                            if ((value != null) && (!"".equals(value))) {
                                                dateTime = formatDate(value);
                                            }
                                            else {
                                                dateTime = value;
                                            }
                                        }
                                        break;
                                    case "who":
                                        who.addProperty(attrDefs.get("taxon_atr"), value);
                                        break;
                                    case "how":
                                        how.addProperty(attrDefs.get("taxon_atr"), value);
                                        break;
                                    default:
                                        // Statements
                                }
                            }
                        }
                    }
                    event.add(WHO, who);
                    event.add(WHERE, where);
                    event.add(WHAT, what);
                    event.add(WHY, why);
                    event.add(WHEN, when);
                    event.add(HOW, how);
                    HashMap<String, Field> responseMap = new HashMap<>(1);
                    responseMap.put(EVENT, Field.create(gson.toJson(event)));
                    record.set(Field.create(responseMap));
                    record.set(PARTY_ID, Field.create(partyId));
                    record.set(PARTY_OWNER, Field.create(partyOwner));
                    record.set(DATE_TIME, Field.create(dateTime));
                    record.set(EVENT_ID, Field.create(eventId));
                    record.set(DATASOURCE_ID, Field.create(datasourceId));
                    record.set(ROW_KEY, Field.create(createRowKey(partyId, partyOwner, dateTime, eventId, datasourceId)));
                    record.set(FLATTENED_EVENT, Field.create(gson.toJson(flattenedEvent)));
                }
            }

        } catch (DateTimeParseException e) {
            throw new OnRecordErrorException(record, Errors.DATETIME_PARSE, e);
        }

        if (multiKey == null) {
            throw new OnRecordErrorException(record, Errors.EVENT_ID_NULL);
        }

        return record;
    }

    private String createRowKey(String partyId, String partyOwner, String dateTime, String eventId, String datasourceId)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(partyId).append("#").append(partyOwner).append("#").append(dateTime).append("#").append(eventId).append("#").append(datasourceId);
        return builder.toString();
    }

    private String formatDate(String dateString)
    {

        LocalDateTime idateTime = LocalDateTime.parse(dateString, iFormatter);
        String outDateString = idateTime.format(oFormatter);
        return outDateString;

    }

    private MultiKey identifyEvent(Map<String, Field> fieldMap){

        boolean identified = true;
        MultiKey multiKey = null;
        logger.debug(fieldMap.keySet().toString());

        for(Map.Entry<MultiKey, Map<String,String>> entry: identifierMap.entrySet()) {
            multiKey = entry.getKey();
            String eventId  = (String)entry.getKey().getKey(0);
            logger.debug("EventId=" + eventId);
            Map<String, String> value = entry.getValue();
            if(value.size() > 0)
            {
                identified = true;
                for(Map.Entry<String, String> data: value.entrySet()) {
                    String param = data.getKey();
                    String val = data.getValue();
                    Field field = fieldMap.get(param);
                    if(field == null)
                        field = fieldMap.get("/" + param);
                    logger.debug("Val=" + val + "Param=" + param);
                    if ((field == null) || !(val).equals(field.getValueAsString()))
                    {
                        identified = false;
                        continue;
                    }
                }
                if(identified)
                    break;
            }

        }
        return identified ? multiKey : null;
    }

    private void populateTaxonomy()
    {
        try {

            logger.info("Loading Event Definition Columns");
            evtColumns = this.findColumnNames(eventQuery);
            logger.info("Loaded Event Definition Columns");
            logger.info("Loading Attribute Definition Columns");
            attrColumns = this.findColumnNames(attrColumnQuery);
            logger.info("Loaded Attribute Definition Columns");
            logger.info("Loading eventMap");
            eventMap.putAll(this.executeQueryForMultiKey(eventQuery,null, evtColumns,  "event_def_id", "evt_map_logic"));
            logger.debug(eventMap.toString());
            logger.info("Loaded eventMap");
            logger.info("Loading attribute Map");
            datasourceId = eventMap.entrySet().iterator().next().getValue().get("datasource_id");

            Map<String,Map<String, Map<String, String>>> innerAttrMap = new HashMap<>();
            for(Map.Entry<MultiKey,Map<String, String>> entry : eventMap.entrySet()) {
                LinkedHashMap<String, Class> paramMap = new LinkedHashMap<>(2);
                paramMap.put(entry.getValue().get("datasource_id"), Integer.class);
                paramMap.put(entry.getValue().get("ds_entity"), String.class);

                innerAttrMap.put(entry.getValue().get("event_def_id"), this.executeQuery(attrQuery, paramMap, attrColumns, "taxon_atr"));
                String json = entry.getValue().get("evt_map_logic");
                Map<String, String> jsonMap = (json == null|| "".equals(json)) ? new HashMap(0) :gson.fromJson(json, Map.class);
                logger.debug(entry.getValue().get("event_def_id") + jsonMap);
                MultiKey multiKey = new MultiKey(entry.getValue().get("event_def_id"), entry.getValue().get("evt_map_logic"));
                identifierMap.put(multiKey, jsonMap);
            }
            attrMap.putAll(innerAttrMap);
            logger.debug(attrMap.toString());
            logger.info("Loaded attrMap");
            logger.debug(identifierMap.toString());


        } catch (Exception e) {
            logger.error("Exception occured while executing: " + eventQuery, e);
        }

    }


    public Map<String, Map<String, String>> executeQuery(String query, Map<String, Class> params, List<String> cols, String key)
            throws WRSDataException
    {

        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = hds.getConnection();
            HashMap<String, Map<String, String>> map = new HashMap<>();
            statement = connection.prepareStatement(query);
            if (params != null && params.size() > 0) {

                int i = 0;
                    for (Map.Entry<String, Class> param : params.entrySet())
                {
                    if (param.getValue().equals(Integer.class)) {
                        statement.setInt(i + 1, Integer.parseInt(param.getKey()));
                    } else {
                        statement.setString(i + 1, param.getKey());
                    }
                    i +=1;
                }
            }

            resultSet =  statement.executeQuery();
            while (resultSet.next())
            {
                HashMap<String, String>  innerMap = new HashMap<>();
                for(String col: cols)
                {
                    innerMap.put(col, resultSet.getString(col));
                }
                map.put(innerMap.get(key), innerMap);
            }

            statement.close();
            connection.close();
            statement= null;
            connection = null;
            return map;

        } catch (SQLException e) {
            logger.error("Exception occured while executing: " + query, e);
            throw new WRSDataException("Exception occured while executing: " + query, e);
        }
        finally {
            DbUtils.closeQuietly(connection, statement, resultSet);
        }
    }


    public Map<MultiKey, Map<String, String>> executeQueryForMultiKey(String query, Map<String, Class> params, List<String> cols, String key1, String key2)
            throws WRSDataException
    {

        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = hds.getConnection();
            HashMap<MultiKey, Map<String, String>> map = new HashMap<>();
            statement = connection.prepareStatement(query);
            if (params != null && params.size() > 0) {

                int i = 0;
                for (Map.Entry<String, Class> param : params.entrySet())
                {
                    if (param.getValue().equals(Integer.class)) {
                        statement.setInt(i + 1, Integer.parseInt(param.getKey()));
                    } else {
                        statement.setString(i + 1, param.getKey());
                    }
                    i +=1;
                }
            }

            resultSet =  statement.executeQuery();
            while (resultSet.next())
            {
                HashMap<String, String>  innerMap = new HashMap<>();
                for(String col: cols)
                {
                    innerMap.put(col, resultSet.getString(col));
                }
                MultiKey multiKey = new MultiKey(innerMap.get(key1), innerMap.get(key2));
                map.put(multiKey, innerMap);
            }

            statement.close();
            connection.close();
            statement= null;
            connection = null;
            return map;

        } catch (SQLException e) {
            logger.error("Exception occured while executing: " + query, e);
            throw new WRSDataException("Exception occured while executing: " + query, e);
        }
        finally {
            DbUtils.closeQuietly(connection, statement, resultSet);
        }
    }


    private List<String> findColumnNames(String query)
            throws WRSDataException
    {
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        List<String> columns = new ArrayList<>();

        try {
            connection = hds.getConnection();
            statement = connection.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            // The column count starts from 1
            for (int i = 1; i <= columnCount; i++) {
                String name = rsmd.getColumnName(i);
                columns.add(name);
            }
        }
        catch (SQLException e) {
            logger.error("Exception occured while executing: " + query, e);
            throw new WRSDataException("Exception occured while executing: " + query, e);
        }
        finally {
            DbUtils.closeQuietly(connection, statement, resultSet);
        }
        return columns;
    }
}