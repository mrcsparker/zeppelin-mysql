/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.mysql;

import static org.apache.zeppelin.mysql.MySqlInterpreter.DEFAULT_JDBC_DRIVER_NAME;
import static org.apache.zeppelin.mysql.MySqlInterpreter.DEFAULT_JDBC_URL;
import static org.apache.zeppelin.mysql.MySqlInterpreter.DEFAULT_JDBC_USER_NAME;
import static org.apache.zeppelin.mysql.MySqlInterpreter.DEFAULT_JDBC_USER_PASSWORD;
import static org.apache.zeppelin.mysql.MySqlInterpreter.DEFAULT_MAX_RESULT;
import static org.apache.zeppelin.mysql.MySqlInterpreter.MYSQL_SERVER_DRIVER_NAME;
import static org.apache.zeppelin.mysql.MySqlInterpreter.MYSQL_SERVER_MAX_RESULT;
import static org.apache.zeppelin.mysql.MySqlInterpreter.MYSQL_SERVER_PASSWORD;
import static org.apache.zeppelin.mysql.MySqlInterpreter.MYSQL_SERVER_URL;
import static org.apache.zeppelin.mysql.MySqlInterpreter.MYSQL_SERVER_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.Before;
import org.junit.Test;

import com.mockrunner.jdbc.BasicJDBCTestCaseAdapter;
import com.mockrunner.jdbc.StatementResultSetHandler;
import com.mockrunner.mock.jdbc.MockConnection;
import com.mockrunner.mock.jdbc.MockResultSet;

/**
 * MySQL interpreter unit tests
 */
public class MySqlInterpreterTest extends BasicJDBCTestCaseAdapter {

  private MySqlInterpreter mysqlInterpreter = null;
  private MockResultSet result = null;

  @Before
  public void beforeTest() {
    MockConnection connection = getJDBCMockObjectFactory().getMockConnection();

    StatementResultSetHandler statementHandler = connection.getStatementResultSetHandler();
    result = statementHandler.createResultSet();
    statementHandler.prepareGlobalResultSet(result);

    Properties properties = new Properties();
    properties.put(MYSQL_SERVER_DRIVER_NAME, DEFAULT_JDBC_DRIVER_NAME);
    properties.put(MYSQL_SERVER_URL, DEFAULT_JDBC_URL);
    properties.put(MYSQL_SERVER_USER, DEFAULT_JDBC_USER_NAME);
    properties.put(MYSQL_SERVER_PASSWORD, DEFAULT_JDBC_USER_PASSWORD);
    properties.put(MYSQL_SERVER_MAX_RESULT, DEFAULT_MAX_RESULT);

    mysqlInterpreter = spy(new MySqlInterpreter(properties));
    when(mysqlInterpreter.getJdbcConnection()).thenReturn(connection);
  }

  @Test
  public void testOpenCommandIndempotency() throws SQLException {
    // Ensure that an attempt to open new connection will clean any remaining connections
    mysqlInterpreter.open();
    mysqlInterpreter.open();
    mysqlInterpreter.open();

    verify(mysqlInterpreter, times(3)).open();
    verify(mysqlInterpreter, times(3)).close();
  }

  @Test
  public void testDefaultProperties() throws SQLException {

    MySqlInterpreter mysqlInterpreter = new MySqlInterpreter(new Properties());

    assertEquals(DEFAULT_JDBC_DRIVER_NAME,
        mysqlInterpreter.getProperty(MYSQL_SERVER_DRIVER_NAME));
    assertEquals(DEFAULT_JDBC_URL, mysqlInterpreter.getProperty(MYSQL_SERVER_URL));
    assertEquals(DEFAULT_JDBC_USER_NAME, mysqlInterpreter.getProperty(MYSQL_SERVER_USER));
    assertEquals(DEFAULT_JDBC_USER_PASSWORD,
        mysqlInterpreter.getProperty(MYSQL_SERVER_PASSWORD));
    assertEquals(DEFAULT_MAX_RESULT, mysqlInterpreter.getProperty(MYSQL_SERVER_MAX_RESULT));
  }

  @Test
  public void testConnectionClose() throws SQLException {

    MySqlInterpreter mysqlInterpreter = spy(new MySqlInterpreter(new Properties()));

    when(mysqlInterpreter.getJdbcConnection()).thenReturn(
        getJDBCMockObjectFactory().getMockConnection());

    mysqlInterpreter.close();

    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
    verifyConnectionClosed();
  }

  @Test
  public void testStatementCancel() throws SQLException {

    MySqlInterpreter mysqlInterpreter = spy(new MySqlInterpreter(new Properties()));

    when(mysqlInterpreter.getJdbcConnection()).thenReturn(
        getJDBCMockObjectFactory().getMockConnection());

    mysqlInterpreter.cancel(null);

    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
    assertFalse("Cancel operation should not close the connection", mysqlInterpreter
        .getJdbcConnection().isClosed());
  }

  @Test
  public void testNullColumnResult() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1000);

    String sqlQuery = "select * from t";

    result.addColumn("col1", new String[] {"val11", null});
    result.addColumn("col2", new String[] {null, "val22"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("col1\tcol2\nval11\t\n\tval22\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testSelectQuery() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1000);

    String sqlQuery = "select * from t";

    result.addColumn("col1", new String[] {"val11", "val12"});
    result.addColumn("col2", new String[] {"val21", "val22"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("col1\tcol2\nval11\tval21\nval12\tval22\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testSelectQueryMaxResult() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1);

    String sqlQuery = "select * from t";

    result.addColumn("col1", new String[] {"val11", "val12"});
    result.addColumn("col2", new String[] {"val21", "val22"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("col1\tcol2\nval11\tval21\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testSelectQueryWithSpecialCharacters() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1000);

    String sqlQuery = "select * from t";

    result.addColumn("co\tl1", new String[] {"val11", "va\tl1\n2"});
    result.addColumn("co\nl2", new String[] {"v\nal21", "val\t22"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TABLE, interpreterResult.type());
    assertEquals("co l1\tco l2\nval11\tv al21\nva l1 2\tval 22\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testExplainQuery() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1000);

    String sqlQuery = "explain select * from t";

    result.addColumn("col1", new String[] {"val11", "val12"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TEXT, interpreterResult.type());
    assertEquals("col1\nval11\nval12\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testExplainQueryWithSpecialCharacters() throws SQLException {

    when(mysqlInterpreter.getMaxResult()).thenReturn(1000);

    String sqlQuery = "explain select * from t";

    result.addColumn("co\tl\n1", new String[] {"va\nl11", "va\tl\n12"});

    InterpreterResult interpreterResult = mysqlInterpreter.interpret(sqlQuery, null);

    assertEquals(InterpreterResult.Code.SUCCESS, interpreterResult.code());
    assertEquals(InterpreterResult.Type.TEXT, interpreterResult.type());
    assertEquals("co\tl\n1\nva\nl11\nva\tl\n12\n", interpreterResult.message());

    verifySQLStatementExecuted(sqlQuery);
    verifyAllResultSetsClosed();
    verifyAllStatementsClosed();
  }

  @Test
  public void testAutoCompletion() throws SQLException {
    mysqlInterpreter.open();
    assertEquals(1, mysqlInterpreter.completion("SEL", 0).size());
    assertEquals("SELECT ", mysqlInterpreter.completion("SEL", 0).iterator().next());
    assertEquals(0, mysqlInterpreter.completion("SEL", 100).size());
  }
}
