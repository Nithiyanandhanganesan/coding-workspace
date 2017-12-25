package test;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
public class test {
public static void main(String args[])
{
	String osVendorId="bilda.donado@yahoo.com";
	//String osVendorId="300288626246729392026111";
	String partyId = UUID.nameUUIDFromBytes(osVendorId.getBytes(StandardCharsets.ISO_8859_1)).toString();
	System.out.println(partyId);
	
}
}
