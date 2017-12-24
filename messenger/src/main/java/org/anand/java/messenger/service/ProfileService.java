package org.anand.java.messenger.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.anand.java.messenger.database.DatabaseClass;
import org.anand.java.messenger.model.Message;
import org.anand.java.messenger.model.Profile;

public class ProfileService {

	private Map<String,Profile> profiles = DatabaseClass.getProfiles();
	
	public ProfileService()
	{
		profiles.put("anand", new Profile(1,"anand","nitiy","anand"));
		profiles.put("kumar", new Profile(2,"kumar","kum","ar"));
	}
	
	public List<Profile> getAllProfiles()
	{
      return new ArrayList<Profile>(profiles.values());
	}
	
	public Profile getProfiles(String profileName)
	{
		return profiles.get(profileName);
	}
	
	public Profile addProfiles(Profile profile)
	{
		profile.setId(profiles.size() + 1);
		profiles.put(profile.getProfileName(), profile);
		return profile;
	}
	
	public Profile updateProfiles(Profile profile)
	{
		if(profile.getProfileName().isEmpty())
		{
			return null;
		}
		profiles.put(profile.getProfileName(), profile);
		return profile;
	}
	
	public Profile removeProfiles(String profileName)
	{
		return profiles.remove(profileName);
	}
	
}
