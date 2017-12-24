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
import org.anand.java.messenger.service.MessageService;

@Path("/messages")
public class MessageResource {
	
  MessageService messageService = new MessageService();	
	
  @GET	
  @Produces(MediaType.APPLICATION_JSON)
  public List<Message> getMessages()
  {
	  return messageService.getAllMessages();
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{messageId}")
  public Message getMessage(@PathParam("messageId") long messageId)
  {
	  return messageService.getMessage(messageId);
  }
  
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Message addMessage(Message message) 
  {
	return messageService.addMessage(message);  
  }
  
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{messageId}")
  public Message updateMessage(@PathParam("messageId") long id, Message message)
  {
      message.setId(id);
	  return messageService.updateMessage(message);	  
  }
  
  @DELETE
  @Path("/{messageId}")
  public void deleteMessage(@PathParam("messageId") long id)
  {
	  messageService.removeMessage(id);
  }
  

}
