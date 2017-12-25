package com.wrs.datamanagement.model;

public class IdData {
    String partyId;
    String id;
    String partyOwner;

    public IdData(String partyId, String id, String partyOwner) {
        this.partyId = partyId;
        this.id = id;
        this.partyOwner = partyOwner;
    }

    public String getPartyId() {
        return partyId;
    }

    public void setPartyId(String partyId) {
        this.partyId = partyId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPartyOwner() {
        return partyOwner;
    }

    public void setPartyOwner(String partyOwner) {
        this.partyOwner = partyOwner;
    }

    @Override
    public String toString() {
        return "IdData{" +
                "partyId='" + partyId + '\'' +
                ", id='" + id + '\'' +
                ", partyOwner='" + partyOwner + '\'' +
                '}';
    }
}
