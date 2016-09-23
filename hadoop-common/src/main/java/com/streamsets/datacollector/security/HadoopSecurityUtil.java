/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.server.util.KerberosUtil;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;

public class HadoopSecurityUtil {

  public static UserGroupInformation getLoginUser(Configuration hdfsConfiguration) throws IOException {
    return LoginUgiProviderFactory.getLoginUgiProvider().getLoginUgi(hdfsConfiguration);
  }

  public static String getDefaultRealm() throws ReflectiveOperationException {
    AccessControlContext accessContext = AccessController.getContext();
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      return KerberosUtil.getDefaultRealm();
    }
  }

  public static UserGroupInformation getProxyUser(String user, UserGroupInformation loginUser) {
    AccessControlContext accessContext = AccessController.getContext();
    synchronized (SecurityUtil.getSubjectDomainLock(accessContext)) {
      return UserGroupInformation.createProxyUser(user, loginUser);
    }
  }

}
