package com.gy.userportrait.entities;

import lombok.Data;

@Data
public class User {
  private String userid;
  private String username ;
  private String sex ;
  private String telphone ;
  private String email;
  private String age ;
  private String registerTime ;
  private  String usetype ;

  public String format(){
    return this.getUserid() +"," + this.getUsername()+"," + this.getSex()+"," + this.getTelphone()+"," + this.getEmail()+"," + this.getAge()
            + "," + this.getRegisterTime() +"," + this.getUsetype();
  }
}
