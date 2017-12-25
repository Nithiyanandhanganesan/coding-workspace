package test;

public enum EnumTemplate {
    ACTIVE("hello active"),
    INACTIVE("hello inactive"),
    DELETED("hello deleted");
	
    private String url;
    
    EnumTemplate(String url)
    {
    	   this.url=url;
    }
    
    public String url()
    {
    		return url;
    }
}

