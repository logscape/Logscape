package com.logscape.play.jmx

import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanParameterInfo, MBeanOperationInfo, MBeanAttributeInfo, ObjectName, MBeanServerConnection}
import com.logscape.play.model.{InvokeResult, Parameter, OperationDescription, BeanOperations, AttributeValue, BeanAttributes, AttributeDescription, UnknownBean}


class Jmx(url: String) {

  var svrURL = url;
  if (url.split(":").length == 2){
    svrURL = "service:jmx:rmi:///jndi/rmi://" + url + "/jmxrmi"

  }

  private val connector: JMXConnector = JMXConnectorFactory.connect(new JMXServiceURL(svrURL))
  private val server = connector.getMBeanServerConnection

  private def getBean(bean:String) = server.getMBeanInfo(new ObjectName(bean))

  def listAttributes(bean: String) =
    BeanAttributes(bean, getBean(bean).getAttributes.toList.map((attrib: MBeanAttributeInfo) => {
      AttributeDescription(attrib.getName, attrib.getType)
    }))

  def getAttributeValue(bean: String, attribute: String) = {
    try {
      val attribute1: AnyRef = server.getAttribute(new ObjectName(bean), attribute)
      AttributeValue(bean, attribute, attribute1)
    } catch {
      case e: Throwable => {
        AttributeValue(bean, attribute, e.toString);
      }
    }
  }

  def getOperations(bean: String) =
    BeanOperations(bean, getBean(bean).getOperations.toList.map((op:MBeanOperationInfo) => {
      OperationDescription(op.getName, op.getDescription, op.getReturnType, op.getSignature.toList.map((item:MBeanParameterInfo) => {
        Parameter(item.getName, item.getType)
      }))
    }))

  def invokeOperation(bean:String, opName:String, params:Array[Any]) = {
    val theOperation = getBean(bean).getOperations.toList.find((op:MBeanOperationInfo) => {
      op.getName == opName && op.getSignature.length == params.length
    })

    theOperation.map((op:MBeanOperationInfo) => {
      try {
        val signature = op.getSignature.map((sig:MBeanParameterInfo) => sig.getType)
        InvokeResult(bean, opName, server.invoke(new ObjectName(bean), opName, params.map((x:Any) => x.asInstanceOf[Object]), signature))
      } catch {
        case e: Throwable => {
          InvokeResult(bean, opName, e.toString);
        }
      }
    }).head
  }

  def getOperation(bean:String, opName:String) =
    getBean(bean).getOperations.toList.find((op:MBeanOperationInfo) => {
      op.getName == opName
    })


  def setAttributeValue(bean:String, attribute:String, value:Any) = {
    server.setAttribute(new ObjectName(bean), new Attribute(attribute, value))
    getAttributeValue(bean, attribute)
  }

  def close() {
    connector.close()
  }

}

object Jmx {
  def apply(url: String) = new Jmx(url)
}
