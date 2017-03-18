 package com.bloom.dbsync.utility;
 
 import javax.xml.bind.annotation.XmlAccessType;
 import javax.xml.bind.annotation.XmlAccessorType;
 import javax.xml.bind.annotation.XmlRootElement;
 
 @XmlRootElement
 @XmlAccessorType(XmlAccessType.FIELD)
 public class IdentityFile
 {
   private String title;
   private String exchangeKey;
   private String licenseKey;
   private String checksum;
 
   public IdentityFile()
   {
     this.title = "";
     this.exchangeKey = "";
     this.licenseKey = "";
     this.checksum = "";
   }
   public String getExchangeKey() {
     return this.exchangeKey;
   }
   public String getLicenseKey() {
     return this.licenseKey;
   }
   public String getTitle() {
     return this.title;
   }
   public String getChecksum() {
     return this.checksum;
   }
   public void setChecksum(String hash) {
     this.checksum = hash;
   }
 }
