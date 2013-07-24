/***************************************************************************
 *  mongodb_log_desig.cpp - MongoDB Logger for designator_integration
 *
 *  Copyright  2012  Jan Winkler, Universität Bremen,
 *                   Institute for Artificial Intelligence
 ****************************************************************************/

/*  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version. A runtime exception applies to
 *  this software (see LICENSE.GPL_WRE file mentioned below for details).
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Library General Public License for more details.
 *
 *  Read the full text in the LICENSE.GPL_WRE file in the doc directory.
 */

// System
#include <iostream>

// ROS
#include <ros/ros.h>

// MongoDB
#include <mongo/client/dbclient.h>

// Designator Integration
#include <designator_integration_msgs/DesignatorRequest.h>
#include <designator_integration_msgs/DesignatorResponse.h>
#include <designators/CDesignator.h>

using namespace std;
using namespace mongo;


DBClientConnection *dbMongoDB;
string strCollection;
string strTopic;

unsigned int in_counter;
unsigned int out_counter;
unsigned int qsize;
unsigned int drop_counter;

static pthread_mutex_t in_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t out_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t drop_counter_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t qsize_mutex = PTHREAD_MUTEX_INITIALIZER;

BSONObj keyValuePairToBSON(CKeyValuePair *ckvpPair) {
  BSONObjBuilder bobBuilder;
  
  if(ckvpPair->type() == STRING) {
    bobBuilder.append(ckvpPair->key(), ckvpPair->stringValue());
  } else if(ckvpPair->type() == FLOAT) {
    bobBuilder.append(ckvpPair->key(), ckvpPair->floatValue());
  } else if(ckvpPair->type() == POSE) {
    geometry_msgs::Pose psPose = ckvpPair->poseValue();
    BSONObjBuilder bobTransform;
    bobTransform.append("position",
			BSON(   "x" << psPose.position.x
			     << "y" << psPose.position.y
			     << "z" << psPose.position.z));
    bobTransform.append("orientation",
			BSON(   "x" << psPose.orientation.x
			     << "y" << psPose.orientation.y
			     << "z" << psPose.orientation.z
			     << "w" << psPose.orientation.w));
    bobBuilder.appendElements(bobTransform.obj());
  } else if(ckvpPair->type() == POSESTAMPED) {
    geometry_msgs::PoseStamped psPoseStamped = ckvpPair->poseStampedValue();
    Date_t stamp = psPoseStamped.header.stamp.sec * 1000 + psPoseStamped.header.stamp.nsec / 1000000;
    
    BSONObjBuilder bobTransformStamped;
    BSONObjBuilder bobTransform;
    bobTransformStamped.append("header",
			       BSON(   "seq" << psPoseStamped.header.seq
				    << "stamp" << stamp
				    << "frame_id" << psPoseStamped.header.frame_id));
    bobTransform.append("position",
    			BSON(   "x" << psPoseStamped.pose.position.x
    			     << "y" << psPoseStamped.pose.position.y
    			     << "z" << psPoseStamped.pose.position.z));
    bobTransform.append("orientation",
    			BSON(   "x" << psPoseStamped.pose.orientation.x
    			     << "y" << psPoseStamped.pose.orientation.y
    			     << "z" << psPoseStamped.pose.orientation.z
    			     << "w" << psPoseStamped.pose.orientation.w));
    bobTransformStamped.append("pose", bobTransform.obj());
    bobBuilder.appendElements(bobTransformStamped.obj());
  } else if(ckvpPair->type() == LIST) {
    BSONObjBuilder bobChildren;
    list<CKeyValuePair*> lstChildren = ckvpPair->children();
    
    for(list<CKeyValuePair*>::iterator itChild = lstChildren.begin();
	itChild != lstChildren.end();
	itChild++) {
      bobChildren.appendElements(keyValuePairToBSON(*itChild));
    }
    
    bobBuilder.append(ckvpPair->key(), bobChildren.obj());
  }
  
  return bobBuilder.obj();
}

void logDesignator(CDesignator *desigLog) {
  BSONObjBuilder bobDesig;
  std::vector<BSONObj> vecChildren;
  
  list<CKeyValuePair*> lstChildren = desigLog->children();
  
  for(list<CKeyValuePair*>::iterator itChild = lstChildren.begin();
      itChild != lstChildren.end();
      itChild++) {
    vecChildren.push_back(keyValuePairToBSON(*itChild));
  }
  
  bobDesig.append("children", vecChildren);
  
  string strDesigType = "unknown";
  switch(desigLog->type()) {
  case ACTION:
    strDesigType = "action";
    break;

  case OBJECT:
    strDesigType = "object";
    break;

  case LOCATION:
    strDesigType = "location";
    break;
    
  default:
    break;
  }
  
  bobDesig.append("type", strDesigType);
  
  dbMongoDB->insert(strCollection, BSON("designator" << bobDesig.obj() <<
					"__recorded" << Date_t(time(NULL) * 1000) <<
					"__topic" << strTopic));
  
  pthread_mutex_lock(&in_counter_mutex);
  ++in_counter;
  pthread_mutex_unlock(&in_counter_mutex);
  pthread_mutex_lock(&out_counter_mutex);
  ++out_counter;
  pthread_mutex_unlock(&out_counter_mutex);
}

void cbDesignatorResponseMsg(const designator_integration_msgs::DesignatorResponse &msg) {
  vector<designator_integration_msgs::Designator> vecDesigs = msg.designators;
  
  for(vector<designator_integration_msgs::Designator>::iterator itDesig = vecDesigs.begin();
      itDesig != vecDesigs.end();
      itDesig++) {
    CDesignator *desigLog = new CDesignator(*itDesig);
    logDesignator(desigLog);
    delete desigLog;
  }
}

void cbDesignatorRequestMsg(const designator_integration_msgs::DesignatorRequest &msg) {
  CDesignator *desigLog = new CDesignator(msg.designator);
  logDesignator(desigLog);
  delete desigLog;
  
  // std::vector<BSONObj> transforms;
  
  // const tf::tfMessage& msg_in = *msg;

  // std::vector<geometry_msgs::TransformStamped>::const_iterator t;
  // for (t = msg_in.transforms.begin(); t != msg_in.transforms.end(); ++t) {
  //   Date_t stamp = t->header.stamp.sec * 1000 + t->header.stamp.nsec / 1000000;
  //   BSONObjBuilder transform_stamped;
  //   BSONObjBuilder transform;
  //   transform_stamped.append("header", BSON("seq" << t->header.seq
  // 					    << "stamp" << stamp
  // 					    << "frame_id" << t->header.frame_id));
  //   transform_stamped.append("child_frame_id", t->child_frame_id);
  //   transform.append("translation", BSON(   "x" << t->transform.translation.x
  // 					 << "y" << t->transform.translation.y
  // 					 << "z" << t->transform.translation.z));
  //   transform.append("rotation", BSON(   "x" << t->transform.rotation.x
  // 				      << "y" << t->transform.rotation.y
  // 				      << "z" << t->transform.rotation.z
  // 				      << "w" << t->transform.rotation.w));
  //   transform_stamped.append("transform", transform.obj());
  //   transforms.push_back(transform_stamped.obj());
  // }

  // mongodb_conn->insert(collection, BSON("transforms" << transforms <<
  //                                       "__recorded" << Date_t(time(NULL) * 1000) <<
  //                                       "__topic" << topic));

  // // If we'd get access to the message queue this could be more useful
  // // https://code.ros.org/trac/ros/ticket/744
  // pthread_mutex_lock(&in_counter_mutex);
  // ++in_counter;
  // pthread_mutex_unlock(&in_counter_mutex);
  // pthread_mutex_lock(&out_counter_mutex);
  // ++out_counter;
  // pthread_mutex_unlock(&out_counter_mutex);
}

void printCount(const ros::TimerEvent &te) {
  unsigned int l_in_counter, l_out_counter, l_drop_counter, l_qsize;

  pthread_mutex_lock(&in_counter_mutex);
  l_in_counter = in_counter; in_counter = 0;
  pthread_mutex_unlock(&in_counter_mutex);

  pthread_mutex_lock(&out_counter_mutex);
  l_out_counter = out_counter; out_counter = 0;
  pthread_mutex_unlock(&out_counter_mutex);

  pthread_mutex_lock(&drop_counter_mutex);
  l_drop_counter = drop_counter; drop_counter = 0;
  pthread_mutex_unlock(&drop_counter_mutex);

  pthread_mutex_lock(&qsize_mutex);
  l_qsize = qsize; qsize = 0;
  pthread_mutex_unlock(&qsize_mutex);
  
  printf("%u:%u:%u:%u\n", l_in_counter, l_out_counter, l_drop_counter, l_qsize);
  fflush(stdout);
}

void initialize() {
  strCollection = "";
  strTopic = "";
  
  in_counter = 0;
  out_counter = 0;
  drop_counter = 0;
  qsize = 0;
}

int main(int argc, char** argv) {
  int nReturnvalue = 0;
  initialize();
  
  string strMongoDBHostname = "localhost";
  string strNodename = "";
  
  bool bDesignatorModeList = false;
  
  char cToken = '\0';
  while((cToken = getopt(argc, argv, "t:m:n:c:d:")) != -1) {
    if((cToken == '?') || (cToken == ':')) {
      printf("Usage: %s -t topic -m mongodb -n nodename -c collection [-d <designator|list>]\n", argv[0]);
      
      nReturnvalue = -1;
      break;
    } else if(cToken == 't') {
      strTopic = optarg;
    } else if(cToken == 'm') {
      strMongoDBHostname = optarg;
    } else if(cToken == 'n') {
      strNodename = optarg;
    } else if(cToken == 'c') {
      strCollection = optarg;
    } else if(cToken == 'd') {
      string strArgument = optarg;
      
      if(strArgument == "designator") {
	bDesignatorModeList = false;
      } else if(strArgument == "list") {
	bDesignatorModeList = true;
      } else {
	ROS_WARN("Designator mode (-d) was set to `%s' which is invalid.", strArgument.c_str());
	ROS_WARN("Valid options are: designator, list. Assuming the standard value: designator");
      }
    }
  }
  
  if(nReturnvalue == 0) {
    if(strTopic == "") {
      ROS_ERROR("No topic given.");
      nReturnvalue = -2;
    } else if(strNodename == "") {
      ROS_ERROR("No node name given.");
      nReturnvalue = -2;
    } else {
      ros::init(argc, argv, strNodename);
      
      if(ros::ok()) {
	ros::NodeHandle nh;
	
	string strError;
	dbMongoDB = new DBClientConnection(true);
	
	if(dbMongoDB->connect(strMongoDBHostname, strError)) {
	  ros::Subscriber subTopic;
	  if(bDesignatorModeList) {
	    subTopic = nh.subscribe(strTopic, 1000, cbDesignatorResponseMsg);
	  } else {
	    subTopic = nh.subscribe(strTopic, 1000, cbDesignatorRequestMsg);
	  }
	  
	  ros::Timer tmrPrintCount = nh.createTimer(ros::Duration(5, 0), printCount);
	  ros::spin();
	  
	  delete dbMongoDB;
	} else {
	  ROS_ERROR("Failed to connect to MongoDB: %s", strError.c_str());
	  nReturnvalue = -1;
	}
	
	ros::shutdown();
      } else {
	ROS_ERROR("ROS initialization failed.");
      }
    }
  }
  
  return nReturnvalue;
}
