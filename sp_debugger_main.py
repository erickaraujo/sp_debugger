# coding=utf-8
# ^initializing this script with codification via Python interpreter

#     SPDebugger. A GUI plugin to Workbench for debug stored procedures on MySQL Server and MariaDB
#     Copyright (C) 2018  Erick Araújo

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.

#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.

#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __builtin__ import any as b_any
from collections import OrderedDict
import os
import traceback
import re
import time
import threading
from multiprocessing.pool import ThreadPool

# Workbench Modules
from wb import *
from workbench.log import log_error, log_warning, log_info
from run_script import RunScriptForm
from workbench.db_utils import MySQLConnection
from workbench.utils import WorkerThreadHelper
import grt
import mforms

# """@ModuleInfo Defining this Python module as a GRT module """
ModuleInfo = DefineModule(
    name="SPDebugger", author="Erick Araujo", version="1.0")

# """@wbplugin defines the name of the plugin to "br.com.tcc.SPDebugger", sets the caption
#        to be shown in places like the menu, where to take input arguments from and also that
#        it should be included in the Utilities submenu in Tools menu."""
# """@wbexport exports the function from the module and also describes the return and argument types of the function"""


@ModuleInfo.plugin(
    "br.ifam.tcc.SPDebugger",
    caption="Open Stored Procedure Debugger",
    input=[wbinputs.currentQueryEditor(),
            wbinputs.currentSQLEditor()],
    pluginMenu="SQL/Utilities", type="standalone")
@ModuleInfo.export(grt.INT, grt.classes.db_query_QueryEditor,
                   grt.classes.db_query_Editor)
def mainForm(current_query_editor, current_sql_editor):
    form = SP_Selector(current_query_editor,
                       current_sql_editor)

class SP_Selector():
    def __init__(self,  query_editor, sql_editor):
        self.sql_editor = sql_editor
        storedProcedure = []

        form_spSelector = mforms.Form(
            None, mforms.FormSingleFrame | mforms.FormResizable | mforms.FormMinimizable)
        form_spSelector.set_title("SPDebugger Tool")

        # newBox(bool horizontal)
        box_mainFrame = mforms.newBox(False)
        box_mainFrame.set_padding(12)
        box_mainFrame.set_spacing(8)

        tbl_layoutTable = mforms.newTable()
        tbl_layoutTable.set_padding(20)
        tbl_layoutTable.set_row_count(2)
        tbl_layoutTable.set_column_count(3)
        tbl_layoutTable.set_row_spacing(7)
        tbl_layoutTable.set_column_spacing(2)

        lbl_defSchema = mforms.newLabel("Default Schema:")
        lbl_defSchema.set_text_align(mforms.MiddleLeft)

        # table.add(View view, INT left_column, INT right_column, INT top_row, INT bottom_row, INT flags)"""
        tbl_layoutTable.add(lbl_defSchema, 0, 1, 0, 1, 0)
        field_schema = mforms.newTextEntry(mforms.NormalEntry)
        field_schema.set_value(self.sql_editor.defaultSchema)
        field_schema.set_read_only(True)
        tbl_layoutTable.add(field_schema, 1, 2, 0, 1,
                            mforms.HFillFlag | mforms.HExpandFlag)

        lbl_chooseSp = mforms.newLabel("Choose a Stored Procedure:")
        lbl_chooseSp.set_text_align(mforms.MiddleLeft)
        tbl_layoutTable.add(lbl_chooseSp, 0, 1, 1, 2, 0)
        self.cb_storedProcedures = mforms.newSelector(mforms.SelectorCombobox)
        self.initStoredProcs()
        tbl_layoutTable.add(self.cb_storedProcedures, 1, 2, 1, 2,
                            mforms.HFillFlag | mforms.HExpandFlag)

        btn_refreshSp = mforms.newButton(mforms.ToolButton)
        btn_refreshSp.set_icon(os.getcwd()+"\\images\\icons\\tiny_refresh.png")
        btn_refreshSp.set_tooltip("Refresh stored procedure list")
        btn_refreshSp.add_clicked_callback(self.refreshStoredProcedures)
        tbl_layoutTable.add(btn_refreshSp, 2, 3, 1, 2, 0)

        box_mainFrame.add(tbl_layoutTable, False, True)
        box_buttons = mforms.newBox(True)
        box_buttons.set_spacing(8)
        btn_ok = mforms.newButton()
        btn_ok.set_text("Start Debugger")
        btn_cancel = mforms.newButton()
        btn_cancel.set_text("Cancel")
        btn_cancel.add_clicked_callback(form_spSelector.close)
        mforms.Utilities.add_end_ok_cancel_buttons(
            box_buttons, btn_ok, btn_cancel)
        box_mainFrame.add_end(box_buttons, False, True)

        form_spSelector.center()
        form_spSelector.set_content(box_mainFrame)
        form_spSelector.set_size(500, 200)

        if not self.cb_storedProcedures.get_string_value().encode("utf-8"):
            mforms.Utilities.show_warning("Error",
                                          "No stored procedures were found in this actual schema <{0}>!".format(
                                              self.sql_editor.defaultSchema),
                                          "OK", "", "")
            return None

        if form_spSelector.run_modal(btn_ok, btn_cancel):
            storedProcedure = self.getStoredProc(
                self.sql_editor.defaultSchema, self.cb_storedProcedures.get_string_value().encode("utf-8"))
            editor_form = UI_Debugger(
                query_editor, self.sql_editor, storedProcedure)

    def initStoredProcs(self):
        script = "SELECT name AS 'sp' FROM mysql.proc WHERE db NOT IN ('{0}', '{1}') and type = 'PROCEDURE' AND db = '{2}';".format(
            'common_schema', 'common_schema_version_control', self.sql_editor.defaultSchema)
        result = grt.root.wb.sqlEditors[0].executeQuery(script, 0)
        sps = []
        if result:
            while result.nextRow():
                sps.append(result.stringFieldValue(0))
            self.cb_storedProcedures.add_items(sps)

    def refreshStoredProcedures(self):
        self.cb_storedProcedures.clear()
        self.initStoredProcs()
        mforms.Utilities_show_message(
            "List refreshed successfully!", "", "OK", "", "")

    def getStoredProc(self, schema_name, str_procedure):
        script = "select specific_name, body from mysql.proc where db = '{0}' and name = '{1}'".format(
            schema_name, str_procedure)
        # script = "SHOW CREATE PROCEDURE {0}.{1}"
        result = grt.root.wb.sqlEditors[0].executeQuery(script, 0)
        sp_object = []
        if result:
            while result.nextRow():
                sp_object.append(result.stringFieldValue(0))  # str_name
                sp_object.append(result.stringFieldValue(1))  # str_body
            return sp_object
        else:
            return None


class UI_Debugger(mforms.Form):
    def __init__(self, current_query_editor, current_sql_editor, stored_procedure_object):
        try:
            """Initializing session variables to control threads, routines and GUI timer refresh."""
            self.current_sqlEditor = current_sql_editor
            self.strp_name = stored_procedure_object[0]
            self.strp_body = stored_procedure_object[1]
            self._listPreBreakpoints = OrderedDict()  # list with breakpoints from SP
            self._listPosBreakpoints = OrderedDict()  # list with breakpoints from GUI
            self.worker_connection = None
            self.debugger_connection = None
            self._watchdog_connection = None  # Connection used only for intern queries
            self._update_timer = None
            self._verbose_debug = False  # Allow a verbose output for debug purposes

            self._workerThread = ThreadPool()
            self._debuggerThread = threading.Thread
            self._watchdogThread = threading.Condition
            self.configs = {}
            self.configs['debug_status'] = 'stop'
            self.configs['debug_first_run'] = False
            self.configs['has_default_breakpoint'] = False

            icon_path = os.getcwd() + "\\images\\"  # icons size 18x18

            """GUI Configuration."""
            self.frm_mainWindow = mforms.Form(None, mforms.FormResizable)
            self.frm_mainWindow.set_title('Stored Procedure Debugger Tool')
            box_mainFrame = mforms.newBox(False)
            box_mainFrame.set_padding(3)
            box_mainFrame.set_spacing(3)
            box_mainContainer = mforms.newBox(True)
            box_mainContainer.set_spacing(10)
            box_mainContainer.set_padding(10)

            panel_codeEditor = mforms.newPanel(mforms.TitledBoxPanel)
            panel_codeEditor.set_title('Stored Procedure')
            box_codeEditor = mforms.newBox(True)
            box_codeEditor.set_padding(2)
            panel_codeEditor.add(box_codeEditor)

            self.code_editor = mforms.newCodeEditor()
            self.code_editor.set_language(mforms.LanguageMySQL)
            self.code_editor.set_size(500, 550)
            self.code_editor.set_text(self.strp_body)
            self.code_editor.set_read_only(True)
            box_codeEditor.add(self.code_editor, True, True)

            # Used only to refresh GUI, invisible
            self.tmp_refresh_txtbox = mforms.newTextBox(mforms.BothScrollBars)
            self.tmp_refresh_txtbox.set_read_only(True)

            panel_output = mforms.newPanel(mforms.TitledBoxPanel)
            panel_output.set_title('Output')
            box_output = mforms.newBox(True)
            box_output.set_size(470, 550)
            box_output.set_padding(1)
            self.textbox_output = mforms.newTextBox(mforms.BothScrollBars)
            self.textbox_output.set_read_only(True)
            self.textbox_output.set_padding(2)
            box_output.add(self.textbox_output, True, True)
            panel_output.add(box_output)

            tb_mainToolBar = mforms.newToolBar(mforms.SecondaryToolBar)
            tbi_runDebug = mforms.newToolBarItem(mforms.ActionItem)

            tbi_runDebug.set_icon(icon_path + "icons\\debug_continue.png")
            tbi_runDebug.set_tooltip('Start debug')
            tbi_runDebug.add_activated_callback(self.rdebug_run)
            tb_mainToolBar.add_item(tbi_runDebug)

            tbi_stopDebug = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stopDebug.set_icon(icon_path + "icons\\debug_stop.png")
            tbi_stopDebug.set_tooltip('Stop debug')
            tbi_stopDebug.add_activated_callback(self.clearOutput)
            tb_mainToolBar.add_item(tbi_stopDebug)

            tb_mainToolBar.add_item(
                mforms.newToolBarItem(mforms.SeparatorItem))

            tbi_breakpoint = mforms.newToolBarItem(mforms.ActionItem)
            tbi_breakpoint.set_icon(
                icon_path + "icons\\query_stop_on_error.png")
            tbi_breakpoint.set_tooltip(
                'Set a breakpoint on line-cursor position')
            tbi_breakpoint.add_activated_callback(self.addRemoveBreakpoint)
            tb_mainToolBar.add_item(tbi_breakpoint)

            tbi_stepInto = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stepInto.set_icon(icon_path + "icons\\debug_step_into.png")
            tbi_stepInto.set_tooltip('Step into deep stack level')
            tbi_stepInto.add_activated_callback(self.toDoActionButton)
            tb_mainToolBar.add_item(tbi_stepInto)

            tbi_stepOut = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stepOut.set_icon(icon_path + "icons\\debug_step_out.png")
            tbi_stepOut.set_tooltip('Step out directly to next breakpoint')
            tbi_stepOut.add_activated_callback(self.toDoActionButton)
            tb_mainToolBar.add_item(tbi_stepOut)

            tbi_stepOver = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stepOver.set_icon(icon_path + "icons\\debug_step.png")
            tbi_stepOver.set_tooltip('Step over to next statement/breakpoint')
            tbi_stepOver.add_activated_callback(self.toDoActionButton)
            tb_mainToolBar.add_item(tbi_stepOver)

            tb_mainToolBar.add_item(
                mforms.newToolBarItem(mforms.SeparatorItem))

            tbi_watchVariable = mforms.newToolBarItem(mforms.ActionItem)
            tbi_watchVariable.set_icon(icon_path + "ui\\edit.png")
            tbi_watchVariable.set_tooltip('Set a variable to watch')
            tbi_watchVariable.add_activated_callback(self.toDoActionButton)
            tb_mainToolBar.add_item(tbi_watchVariable)

            btn_cancel = mforms.newButton()
            btn_cancel.set_text('Close Debugger')
            btn_cancel.add_clicked_callback(self.btnCloseWindow)

            box_buttonBox = mforms.newBox(True)
            box_buttonBox.set_padding(10)
            box_buttonBox.add_end(btn_cancel, False, True)

            box_mainContainer.add(panel_codeEditor, False, True)
            box_mainContainer.add(panel_output, True, True)
            box_mainFrame.add(tb_mainToolBar, True, False)
            box_mainFrame.add(box_mainContainer, True, False)
            box_mainFrame.add_end(box_buttonBox, False, False)

            if self.checkFrameworkStatus():
                self.addCompiledDebug()
                self.frm_mainWindow.set_content(box_mainFrame)
                self.frm_mainWindow.show()
                self.frm_mainWindow.center()
                self.frm_mainWindow.add_closed_callback(self.frmCloseWindow)
                self._update_timer = mforms.Utilities.add_timeout(
                    0.1, self._update_ui)

        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    """ Framework Statuses """

    def checkFrameworkStatus(self):
        script = "select exists(select 1 from information_schema.SCHEMATA where schema_name = 'common_schema') 'exists';"
        result_set = grt.root.wb.sqlEditors[0].executeQuery(script, 0)
        if result_set:
            while result_set.nextRow():
                framework_installed = int(result_set.stringFieldValue(0))
            log_info(" ;var framework_installed = " + str(framework_installed))
        if framework_installed:
            log_info(" ;framework found!")
            return True
        else:
            mforms.Utilities.show_warning("Warning!",
                                          "CommonSchema not found! Press OK to start importer", "OK", "", "")
            self.installFramework()

    def installFramework(self):
        try:
            if self.current_sqlEditor.serverVersion.majorNumber > 5 or (self.current_sqlEditor.serverVersion.majorNumber == 5 and self.current_sqlEditor.serverVersion.minorNumber >= 1):
                pass
            else:
                mforms.Utilities.show_warning("Incompatibility version!", "Your MySQL Server version " +
                                              "({0}.{1}) is incompatible with this tool. It's recommended MySQL Version 5.1 or newer.".
                                              format(
                                                  str(self.current_sqlEditor.serverVersion.majorNumber),
                                                  str(self.current_sqlEditor.serverVersion.minorNumber)),
                                              "OK", "", "")
                return False

            try:
                formImporter = RunScriptForm(self.current_sqlEditor)
                status_install = formImporter.run()
                return status_install
            except:
                mforms.Utilities.show_warning(
                    "Import error!", ""+str(traceback.format_exc()), "OK", "", "")
                return False

        except:
            mforms.Utilities.show_warning(
                "Error in identifying MySQL Version!", ""+str(traceback.format_exc()), "OK", "", "")
            return False

    """ GUI Events"""

    # Handle events on 'close' button, 'frmCloseWindow' trigger whenever the main form closes.
    # No needs to call the function here, if so it'll be called twice
    def btnCloseWindow(self):
        if mforms.Utilities.show_message("Closing debugger tool", "Are you sure?", "Continue", "Cancel", "") == mforms.ResultOk:
            self.frm_mainWindow.close()

    # Handle events when 'X' button in top window is pressed
    def frmCloseWindow(self):
        if self._update_timer:
            mforms.Utilities.cancel_timeout(self._update_timer)
            log_info("update_timer canceled successfully.")
        self.removeCompiledDebug()

    def toDoActionButton(self, s):
        try:
            mforms.Utilities.show_warning(
                "Buttons Clicked", "TO DO THIS FUNCTION", "OK", "", "")
            self._debug_printToOutput("clicked")
        except:
            raise

    def printToOutput(self, text, scroll_to_end=True):
        if text:
            self.textbox_output.append_text_and_scroll(
                ">> " + str(text) + "\n", scroll_to_end)

    def clearOutput(self, a):
        self.textbox_output.clear()
       

    # Only used in development mode
    def _debug_printToOutput(self, text):
        if text and self._verbose_debug:
            traceb = traceback.extract_stack(limit=2)
            self.printToOutput("(%s) %s:%s >> %s \n" %
                               (str(threading.current_thread().getName()),
                                traceb[-2][2],  # Get current function name
                                # Get current line from source code
                                traceb[-2][1],
                                str(text))
                               )

    # Output MySQLResultSet in a 'pretty' format
    def _printFormattedText(self, list_results):  # TO DO IMPROVEMENT
        statement_number = 1
        result_text = "Result: \n"
        identation = " " * 5
        linebreak = "\n"
        column_separator = " | "
        column_point = "+ "
        row_pointer = "> "

        for result in list_results:
            try:
                row_line = "-"*(len(identation)*3)
                result_text += column_point + row_line + linebreak
                result_text += column_separator + \
                    str(statement_number) + ". Statement " + linebreak
                for c in range(result.numFields()):
                    c += 1  # Column index
                    column_name = " Column: " + result.fieldName(c)
                    result_text += column_point + row_line + linebreak
                    result_text += column_separator + column_name + linebreak
                    result_text += column_point + row_line + linebreak
                    if result.numRows() > 0:
                        while result.nextRow():
                            row = column_separator + row_pointer + \
                                result.stringByIndex(c) + linebreak
                            result_text += row
                    else:
                        row = column_separator + row_pointer + 'No row returned' + linebreak
                        result_text += row

                statement_number += 1
            except:
                self.printToOutput("0 row(s) returned")
                result_text = None

        result_text += column_point + row_line + linebreak + linebreak + linebreak
        if result_text:
            self.printToOutput(result_text)

    # Called by mForms GUI.
    def _update_ui(self):  # TO DO - IMPLEMENTS?
        # Refreshing gui.
        if self.tmp_refresh_txtbox.get_string_value() is not None:
            txt = self.tmp_refresh_txtbox.get_string_value()
        else:
            txt = '.'

        if self.configs['debug_status'] == 'run':
            txt = 'running'
            if not self._isWorkerWaiting():
                txt = 'worker being executed'
            else:
                txt = 'worker waiting'
        else:
            txt = 'not running'

        self.tmp_refresh_txtbox.append_text_and_scroll(txt, True)
        return True

    # Code_editor is 0-based line index
    def addRemoveBreakpoint(self, arb):
        old_position = self.code_editor.get_caret_pos()
        line = self.code_editor.line_from_position(
            self.code_editor.get_caret_pos())
        self._setBreakpointOnGUI(line)

    def _setBreakpointOnGUI(self, line):
        if self.code_editor.has_markup(mforms.LineMarkupBreakpoint, line):
            self.code_editor.remove_markup(mforms.LineMarkupBreakpoint, line)
            self._debug_printToOutput("Removed breakpoint line " + str(line+1))
            if line in self._listPosBreakpoints:
                del self._listPosBreakpoints[line]

        else:
            self.code_editor.show_markup(mforms.LineMarkupBreakpoint, line)
            self._debug_printToOutput("Added breakpoint line " + str(line+1))
            if line in self._listPreBreakpoints:
                if not line in self._listPosBreakpoints:
                    self._listPosBreakpoints[line] = self._listPreBreakpoints[line]

    # Get breakpoint position on SP and mark them in GUI editor
    # Only called once
    def _searchAndSetBreakpointOnGUI(self):
        script = "CALL common_schema.rdebug_show_routine('{0}', '{1}');".format(
            self.current_sqlEditor.defaultSchema, self.strp_name)

        first_position = 0
        actual_position = 0
        text_pattern = r'(\[:(\d+)\])'
        regex_pattern = re.compile(text_pattern, re.IGNORECASE)

        list_with_id = OrderedDict()
        # Temp variables to sync line and breakpoint IDs
        line_index = 0
        line_content = ''

        try:
            result = self.workerExecuteSingleQuery(script)
            if result:
                while result.nextRow():
                    line_content = result.stringByIndex(1)
                    regex_result = regex_pattern.findall(line_content)
                    if regex_result:
                        self._listPreBreakpoints[line_index] = ''.join(
                            [x[1] for x in regex_result])
                        self._setBreakpointOnGUI(line_index)
                    line_index += 1
        except:
            raise

    # Parameter Form Configuration."""
    def _inputParametersForm(self):
        mainForm_parameters = mforms.Form(None, mforms.FormToolWindow)
        mainForm_parameters.set_title(
            'Parameters in ({0})'.format(self.strp_name))
        box_parameters = mforms.newBox(False)
        box_parameters.set_spacing(8)
        box_parameters.set_padding(8)

        table_parameters = mforms.newTable()
        table_parameters.set_padding(10)
        table_parameters.set_row_spacing(8)
        table_parameters.set_column_spacing(2)
        table_parameters.set_column_count(3)

        _list_parameters = []
        dict_textentry = {}
        dict_params = OrderedDict()
        dict_ParamOut = OrderedDict()
        dict_ParamIn = OrderedDict()

        regex_pattern = '(\w+);;(\w+);;(\w+)'
        pattern = re.compile(regex_pattern, re.IGNORECASE)

        try:
            self._workerThread = ThreadPool(processes=1)
            async_result = self._workerThread.apply_async(
                self._appendParametersToList, args=(_list_parameters,))
        except:
            raise

        _list_parameters = async_result.get()

        self._debug_printToOutput('lista->' + str(_list_parameters))

        if _list_parameters:
            # if self._append
            left = 0
            top = 0
            right = 1
            bottom = 1
            table_parameters.set_row_count(len(_list_parameters))
            for param in _list_parameters:
                m = re.match(regex_pattern, param)

                if m.group(1) == "IN":
                    label_param_name = mforms.newLabel(
                        str(m.group(1))+" " + str(m.group(2)))
                    label_param_name.set_text_align(mforms.NoAlign)

                    param_textbox = mforms.newTextEntry(mforms.NormalEntry)
                    param_textbox.set_size(125, 20)

                    param_type = mforms.newLabel("("+str(m.group(3))+")")

                    """Adding each param_textbox variable in a dictionary for
                        dinamically each input."""
                    dict_textentry[m.group(2)] = param_textbox

                    dict_params[m.group(2)] = m.group(2)
                    table_parameters.add(
                        label_param_name, 0, 1, top, bottom, 0)
                    table_parameters.add(
                        param_textbox, 1, 2, top, bottom, 0)
                    table_parameters.add(param_type, 2, 3, top, bottom, 0)

                    dict_ParamIn[m.group(2)] = param_textbox
                    top += 1
                    bottom += 1

                elif m.group(1) == "OUT":
                    dict_ParamOut[m.group(2)] = m.group(2)
                    dict_params[m.group(2)] = "@"+m.group(2)

            box_parameters.add(table_parameters, False, False)
            btn_param_ok = mforms.newButton()
            btn_param_ok.set_text("OK")
            btn_param_cancel = mforms.newButton()
            btn_param_cancel.set_text("Cancel")

            mforms.Utilities.add_end_ok_cancel_buttons(
                box_parameters, btn_param_ok, btn_param_cancel)
            mainForm_parameters.set_content(box_parameters)
            mainForm_parameters.center()
            try:
                if mainForm_parameters.run_modal(btn_param_ok, btn_param_cancel):
                    self._execute_sp(dict_params, dict_ParamIn, dict_ParamOut)
            except:
                raise
        else:
            self._execute_sp(False, False, False)

    def _appendParametersToList(self, _list_parameters):
        params = ''
        routine_type = 'PROCEDURE'
        script_parameters = """SELECT parameter_mode, parameter_name, data_type FROM information_schema.parameters
        WHERE routine_type = '{0}'
        AND specific_schema = '{1}'
        AND specific_name = '{2}'; """.format(
            routine_type.upper(),
            self.current_sqlEditor.defaultSchema,
            self.strp_name)
        try:
            result_parameters = self.debuggerExecuteSingleQuery(
                script_parameters, False)
            if result_parameters:
                while result_parameters.nextRow():
                    params = ''.join([result_parameters.stringByName('parameter_mode'),
                                      ";;", result_parameters.stringByName(
                        'parameter_name'),
                        ";;", result_parameters.stringByName('data_type')])
                    _list_parameters.append(params)
        except:
            raise

        if not _list_parameters or b_any("in;;" in st for st in _list_parameters):
            return _list_parameters
        else:
            return _list_parameters

    # FIX -TO DO- WHEN CALLED TO EXECUTE A STEP
    # SET A STEP INTO WHEN NO BREAKPOINT IS FOUND
    # AND THEN EXITS DEBUG
    def _execute_sp(self, params, params_in, params_out):
        script = "call {0}.{1}(".format(
            self.current_sqlEditor.defaultSchema, self.strp_name)
        script_params_out = ""
        _list_out_params = []
        if params_out:
            i = 1
            script_params_out = "SET "
            for index, textout in params_out.items():
                t = "@"+index
                _list_out_params.append(t)
                script_params_out += t + " = NULL"
                script_params_out += ';' if i == len(params_out) else ', '
                i += 1
            res = self.debuggerExecuteMultiResultQuery(script_params_out)

        if params:
            x = 1
            for k, param_name in params_in.items():
                script += "'" + param_name.get_string_value() + "'"
                script += ", " if x != len(params) else ''
                x += 1

            z = 1
            for k, param_name in params_out.items():
                script += "@" + param_name
                script += ", " if z != len(params_out) else ''
                z += 1

        script += ');'

        resultsets = self.debuggerExecuteMultiResultQuery(script)
        self._printFormattedText(resultsets)

    """
        Connections with MySQL Workbench API.
    """

    def setWorkerConnectionID(self):
        try:
            script = "SELECT CONNECTION_ID();"
            result = self.workerExecuteSingleQuery(script)
            if result and result.nextRow():
                self._debug_printToOutput(
                    "Successfully raised a Worker Session")
                self._session_worker_id = result.stringByName(
                    'CONNECTION_ID()')
                self._debug_printToOutput(
                    "Worker ID: " + self._session_worker_id)
        except:
            raise

    def getWorkerConnectionID(self):
        return self._session_worker_id

    def getDebuggerConnectionID(self):
        return self._session_debugger_id

    def setDebuggerConnectionID(self):
        try:
            script = "SELECT CONNECTION_ID();"
            result = self.debuggerExecuteSingleQuery(script)
            if result and result.nextRow():
                self._debug_printToOutput(
                    "Successfully raised a Debugger Session")
                self._session_debugger_id = result.stringByName(
                    'CONNECTION_ID()')
                self._debug_printToOutput(
                    "Debugger ID: " + self._session_debugger_id)
        except:
            raise

    # Creates a new connection to worker and debugger session starts
    def runWorkerConnector(self):
        try:
            info = grt.root.wb.rdbmsMgmt.storedConns[0]
            self.worker_connection = MySQLConnection(info)
            self.worker_connection.connect()
            if self.worker_connection.is_connected:
                self.setWorkerConnectionID()
            else:
                mforms.Utilities.show_warning(
                    "Connection failed!", "Worker session failed to connect!", "OK", "", "")
        except:
            raise

    def runDebuggerConnector(self):
        try:
            info = grt.root.wb.rdbmsMgmt.storedConns[0]
            self.debugger_connection = MySQLConnection(info)
            self.debugger_connection.connect()
            if self.debugger_connection.is_connected:
                self.setDebuggerConnectionID()
            else:
                mforms.Utilities.show_warning(
                    "Connection Failed!", "Debugger session failed to connect!", "OK", "", "")
        except:
            raise

    # Used only for intern queries, preventing 'Commands Out Of Sync'
    def _watchdogConnection(self):
        try:
            info = grt.root.wb.rdbmsMgmt.storedConns[0]
            self._watchdog_connection = MySQLConnection(info)
            self._watchdog_connection.connect()
            if self._watchdog_connection.is_connected:
                try:
                    scp = "SELECT CONNECTION_ID()"
                    result = self._watchdogExecuteSingleQuery(scp)
                    if result and result.nextRow():
                        self._watchdog_connection_id = result.stringByName(
                            'CONNECTION_ID()')
                        self._debug_printToOutput(
                            'watchdog connection started ({0})'.format(self._watchdog_connection_id))
                except:
                    raise
            else:
                mforms.Utilities.show_warning(
                    "Connection Failed!", "Debugger session failed to connect!", "OK", "", "")
        except:
            raise

    def debuggerExecuteSingleQuery(self, script, printDebug=True):
        try:
            if printDebug:
                self._debug_printToOutput(
                    'Executing simple script via (' +
                    threading.currentThread().getName()+') \n '
                    + 'script: ' + script)
            result = self.debugger_connection.executeQuery(script)
            if result:
                return result
        except:
            raise

    def workerExecuteSingleQuery(self, script, printDebug=True):
        try:
            if printDebug:
                self._debug_printToOutput(
                    'Executing simple script via (' +
                    threading.currentThread().getName()+')  \n '
                    + 'script: ' + script)
            result = self.worker_connection.executeQuery(script)
            if result:
                return result
        except:
            raise

    def _watchdogExecuteSingleQuery(self, script):
        try:
            result = self._watchdog_connection.executeQuery(script)
            if result:
                return result
        except:
            raise

    # Return a list of MySQLResult object
    def debuggerExecuteMultiResultQuery(self, script, printDebug=True):
        try:
            if printDebug:
                self._debug_printToOutput(
                    'Executing multi result script via (' +
                    threading.currentThread().getName()+') \n '
                    + 'script: ' + script)
            result = self.debugger_connection.executeQueryMultiResult(script)
            if result:
                return result
        except:
            raise

    def workerExecuteMultiResultQuery(self, script, printDebug=True):
        try:
            if printDebug:
                self._debug_printToOutput(
                    'Executing multi result script via (' +
                    threading.currentThread().getName()+')  \n '
                    + 'script: ' + script)
            result = self.worker_connection.executeQueryMultiResult(script)
            if result:
                return result
        except:
            raise

    def _watchdogExecuteMultiResultQuery(self, script):
        try:
            result = self._watchdog_connection.executeQueryMultiResult(script)
            if result:
                return result
        except:
            raise

    def addCompiledDebug(self):
        self.runDebuggerConnector()
        self.runWorkerConnector()
        self._watchdogConnection()
        self.compileDebugOnSp(True)
        self._searchAndSetBreakpointOnGUI()
        self.rdebug_start(self.getWorkerConnectionID())

    def removeCompiledDebug(self):
        self.compileDebugOnSp(False)
        self.rdebug_stop()
        if self.debugger_connection.is_connected:
            self.debugger_connection.disconnect()
        if self.worker_connection.is_connected:
            self.worker_connection.disconnect()
        if self._watchdog_connection.is_connected:
            self._watchdog_connection.disconnect()

        log_info('  Called remove debug from SP')

    def compileDebugOnSp(self, booleanAction):
        sch_name = self.current_sqlEditor.defaultSchema
        sp_name = self.strp_name
        params = "'{0}', '{1}', {2}".format(
            sch_name, sp_name, str(booleanAction))
        script = "CALL common_schema.rdebug_compile_routine({0});".format(
            params)
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if not result:
                mforms.Utilities.show_message(
                    "Error!", "Could not compile routine with debug.", "OK", "", "")
        except:
            raise

    """
        Connections to RDEBUG API
    """

    def rdebug_start(self, session_id):
        script = "CALL common_schema.rdebug_start({0});".format(session_id)
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result:
                self._debug_printToOutput(
                    "rdebug has started! in {0}".format(session_id))
        except:
            mforms.Utilities.show_warning(
                "Error!", "rdebug failed to start at {0} connection id!".format(session_id), "", "CANCEL", "")
            raise

    def rdebug_stop(self):
        script = "CALL common_schema.rdebug_stop();"
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result:
                self._debug_printToOutput("rdebug has stopped!")
        except:
            mforms.Utilities.show_warning(
                "Error!", "rdebug failed to stop", "", "CANCEL", "")
            raise

    def rdebug_set_verbose(self, boolean):
        script = 'CALL common_schema.rdebug_set_verbose({0});'.format(
            str(boolean))
        try:
            result = self.debuggerExecuteSingleQuery(script)
        except:
            raise

    def rdebug_real_run(self):
        script = "CALL common_schema.rdebug_run();"
        try:
            with self._watchdogThread:
                result = self.debuggerExecuteMultiResultQuery(script)
                if result:
                    for res in result:
                        self.printToOutput(res)
        except:
            raise

    def rdebugStepInto(self, sst):
        self._rdebugSetStep('into')

    def rdebugStepOut(self, sst):
        self._rdebugSetStep('out')

    def rdebugStepOver(self, sst):
        self._rdebugSetStep('over')

    def _rdebugSetStep(self, step):
        script = 'CALL common_schema.rdebug_step_{0}();'.format(step)
        try:
            result = self.debuggerExecuteMultiResultQuery(script)
            if result:
                for res in result:
                    self.printToOutput(res)
        except:
            raise

    # add_bp: Boolean -> Add = True | Remove = False
    def _rdebug_set_breakpoint(self, position, add_bp):
        script_add = "call common_schema.rdebug_set_breakpoint('{0}', '{1}', {2}, null, {3});".format(
            self.current_sqlEditor.defaultSchema,
            self.strp_name,
            position,
            add_bp)
        try:
            result2 = self.debuggerExecuteSingleQuery(script_add)
            if result2:
                self._debug_printToOutput("Breakpoint added")

        except:
            mforms.Utilities.show_message(
                "Error", "Error when trying to add breakpoint!", "OK", "", "")
            raise

    def rdebug_run(self, rn):  # TO DO IMPROVEMENT ON BREAKPOINTS
        # _list_breakpoints = self._rdebugCheckBreakpoints()
        # if not (_list_breakpoints) or not (_list_breakpoints.numRows() > 0):
        #     self._debug_printToOutput("No pre-breakpoints found!")
        #     self._rdebugSetLastBreakpoint()
        # self._debuggerThread = threading.Thread(
        #     name='debugger', target=self._inputParametersForm, args=('',))
        # self._debuggerThread.start()
        self._inputParametersForm()

    # Setting last breakpoint automatically when
    # No breakpoint was found on GUI
    def _rdebugSetLastBreakpoint(self):
        script = "SELECT statement_id FROM common_schema._rdebug_routine_statements WHERE routine_schema = '{0}' AND routine_name = '{1}' ORDER BY statement_id DESC LIMIT 1;".format(
            self.current_sqlEditor.defaultSchema,
            self.strp_name)
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result and result.nextRow():
                self._rdebug_set_breakpoint(
                    result.stringByName('statement_id'), True)
        except:
            mforms.Utilities.show_message(
                "Error", "Error calling breakpoint setter!", "OK", "", "")
            raise

    # Check all breakpoints setted via GUI, return a MySQL ResultSet
    def _rdebugCheckBreakpoints(self):
        script = "SELECT statement_id FROM common_schema._rdebug_breakpoint_hints WHERE worker_id = '{0}' AND routine_schema = '{1}' AND routine_name = '{2}';".format(
            self.getWorkerConnectionID(),
            self.current_sqlEditor.defaultSchema,
            self.strp_name
        )
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result:
                return result
        except:
            self.printToOutput("Failed to check breakpoints!")
            raise

    def _isWorkerWaiting(self):
        script = "SELECT IFNULL(MAX(command)='Sleep', true) as 'checkStatus' FROM information_schema.processlist WHERE id={0};".format(
            self.getWorkerConnectionID())
        res = self._watchdogExecuteSingleQuery(script)
        if res and res.nextRow():
            return int(res.stringByName('checkStatus'))
