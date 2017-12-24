package org.anand.java.messenger.resources;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.anand.java.messenger.model.Message;
import org.anand.java.messenger.model.Profile;
import org.anand.java.messenger.service.MessageService;
import org.anand.java.messenger.service.ProfileService;

@Path("/profiles")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ProfileResource {
	
	  ProfileService profileService = new ProfileService();	
		
	  @GET	
	  public List<Profile> getMessages()
	  {
		  return profileService.getAllProfiles();
	  }
	  
	  @GET
	  @Path("/{profileName}")
	  public Profile getProfile(@PathParam("profileName") String profileName)
	  {
		  return profileService.getProfiles(profileName);
	  }
	  
	  @POST	  
	  public Profile addProfile(Profile profile) 
	  {
		return profileService.addProfiles(profile);  
	  }
	  
	  @PUT
	  @Path("/{profileName}")
	  public Profile updateProfile(@PathParam("profileName") String profileName, Profile profile)
	  {
		  profile.setProfileName(profileName);
		  return profileService.updateProfiles(profile);	  
	  }
	  
	  @DELETE
	  @Path("/{profileName}")
	  public void deleteProfile(@PathParam("profileName") String profileName)
	  {
		  profileService.removeProfiles(profileName);
	  }
	  

}
