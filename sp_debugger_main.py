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
            self._debuggerThread = ThreadPool()
            self._watchdogThread = ThreadPool()
            self.configs = {}
            self.configs['debug_status'] = 'stop'

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
            self.code_editor.set_size(480, 550)
            self.code_editor.set_text(self.strp_body)
            self.code_editor.set_read_only(True)
            box_codeEditor.add(self.code_editor, True, True)

            # Used only to refresh GUI, invisible
            self.tmp_refresh_txtbox = mforms.newTextBox(mforms.BothScrollBars)
            self.tmp_refresh_txtbox.set_read_only(True)

            panel_output = mforms.newPanel(mforms.TitledBoxPanel)
            panel_output.set_title('Output')
            box_output = mforms.newBox(True)
            box_output.set_size(515, 550)
            box_output.set_padding(1)
            self.textbox_output = mforms.newTextBox(mforms.BothScrollBars)
            self.textbox_output.set_read_only(True)
            self.textbox_output.set_monospaced(False)
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
            tbi_stopDebug.add_activated_callback(self.rdebug_stop)
            tb_mainToolBar.add_item(tbi_stopDebug)

            tb_mainToolBar.add_separator_item('Separator')

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
            tbi_stepInto.add_activated_callback(self.rdebugStepInto)
            tb_mainToolBar.add_item(tbi_stepInto)

            tbi_stepOut = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stepOut.set_icon(icon_path + "icons\\debug_step_out.png")
            tbi_stepOut.set_tooltip('Step out directly to next breakpoint')
            tbi_stepOut.add_activated_callback(self.rdebugStepOut)
            tb_mainToolBar.add_item(tbi_stepOut)

            tbi_stepOver = mforms.newToolBarItem(mforms.ActionItem)
            tbi_stepOver.set_icon(icon_path + "icons\\debug_step.png")
            tbi_stepOver.set_tooltip('Step over to next statement/breakpoint')
            tbi_stepOver.add_activated_callback(self.rdebugStepOver)
            tb_mainToolBar.add_item(tbi_stepOver)

            tb_mainToolBar.add_separator_item('Separator')

            tbi_watchVariable = mforms.newToolBarItem(mforms.ActionItem)
            tbi_watchVariable.set_icon(icon_path + "ui\\edit.png")
            tbi_watchVariable.set_tooltip(
                'Set a variable to watch/change value')
            tbi_watchVariable.add_activated_callback(self.inputVariablesForm)
            tb_mainToolBar.add_item(tbi_watchVariable)

            tbi_clearOutput = mforms.newToolBarItem(mforms.ActionItem)
            tbi_clearOutput.set_icon(icon_path + "icons\\wb_rubber.png")
            tbi_clearOutput.set_tooltip('Clear output log')
            tbi_clearOutput.add_activated_callback(self.clearOutput)
            tb_mainToolBar.add_item(tbi_clearOutput)

            btn_cancel = mforms.newButton()
            btn_cancel.set_text('Close Debugger')
            btn_cancel.add_clicked_callback(self.btnCloseWindow)

            box_buttonBox = mforms.newBox(True)
            box_buttonBox.set_padding(10)
            box_buttonBox.add_end(btn_cancel, False, True)

            box_statuses = mforms.newBox(True)
            box_statuses.set_spacing(5)
            box_statuses.set_padding(3)
            lb_progressLabel = mforms.newLabel("Status:")
            self._progress = mforms.newLabel('')

            box_statuses.add(lb_progressLabel, False, False)
            box_statuses.add(self._progress, False, False)

            box_mainContainer.add(panel_codeEditor, False, True)
            box_mainContainer.add(panel_output, True, True)

            box_mainFrame.add(tb_mainToolBar, True, False)
            box_mainFrame.add(box_mainContainer, True, False)
            box_mainFrame.add(box_statuses, True, False)
            box_mainFrame.add_end(box_buttonBox, False, False)

            if self.checkFrameworkStatus():
                self.addCompiledDebug()
                self.frm_mainWindow.set_content(box_mainFrame)
                self.frm_mainWindow.show()
                self.frm_mainWindow.center()
                self.frm_mainWindow.add_closed_callback(self.frmCloseWindow)
                self._update_timer = mforms.Utilities.add_timeout(
                    0.1, self._update_ui)
                self.printToOutput("Welcome to SPDebugger")

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
            log_info("... var framework_installed = " +
                     str(framework_installed))
        if framework_installed:
            log_info("... framework found!")
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
            log_info("... update_timer canceled successfully.")
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
        self.printToOutput("Welcome to SPDebugger")

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
    def _printFormattedText(self, resultsets, resultCaller='', debugger=False):
        identation = " " * 5
        result_text = "Executing '"+resultCaller+"':\n"
        output = []

        try:
            if debugger and len(resultsets) > 1:
                resultsets = [resultsets[1]]
                debugger = False

            if not debugger:
                for result in resultsets:
                    output = []
                    line = []
                    column_lengths = []
                    if result.numFields() >= 1:
                        ncolumns = result.numFields()

                        # setting ncolumns + 1 because of range() behaviour
                        # fieldName() index start at 1 ...
                        for column_index in range(1, ncolumns+1):
                            column_name = result.fieldName(column_index)
                            line.append(column_name + identation)
                            column_lengths.append(
                                len(column_name) + len(identation))

                        separator = []
                        for c in column_lengths:
                            separator.append("-"*(c+1))
                        separator = " + ".join(separator)
                        output.append("+ "+separator+" +")

                        line = " | ".join(line)
                        output.append("| "+line+" |")

                        output.append("+ "+separator+" +\n")

                        ok = result.firstRow()
                        # if ok:
                        result_text += '\n'.join(output)

                        last_flush = 0
                        rows = []
                        while ok:
                            line = []
                            for i in range(1, ncolumns+1):
                                value = result.stringByIndex(i)
                                if value is None:
                                    value = "NULL"
                                # column_lenghts to i-1: python lists index start with 0
                                line.append(value.ljust(column_lengths[i-1]))
                            line = " | ".join(line)
                            rows.append("| "+line+" |")

                            # flush text every 1/2s
                            if time.time() - last_flush >= 0.5:
                                last_flush = time.time()
                                result_text += "\n".join(rows)+"\n"
                                rows = []
                            ok = result.nextRow()

                        if rows:
                            result_text += "\n".join(rows)+"\n"
                        result_text += "+ "+separator+" +\n"

                        result_text += "%i rows\n\n" % (result.numRows())

            if len(output) > 0:
                self.printToOutput(result_text)
            else:
                if debugger == True:
                    self.printToOutput(result_text + "\n" +
                                       identation + "No rows returned.\n\n")

                result = self.worker_async_result.get(0.2)
                self._printFormattedText(result, "worker")

        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")

    # Called by mForms GUI.
    def _update_ui(self):
        self._watchdogThread = ThreadPool()
        workerWorkingAsync = self._watchdogThread.apply_async(
            self._checkWorkerWaiting)
        isWorkerWorking = workerWorkingAsync.get()

        if not isWorkerWorking:
            self._progress.set_text('Not running...')
            self.configs['debug_status'] = 'stop'
        else:
            self._progress.set_text('Running...')
            self.configs['debug_status'] = 'run'

        return True

    # Code_editor is 0-based line index
    def addRemoveBreakpoint(self, arb):
        line = self.code_editor.line_from_position(
            self.code_editor.get_caret_pos())
        self._setBreakpointOnGUI(line)

    def _setBreakpointOnGUI(self, line):
        # Remove markup of breakpoint in line
        if self.code_editor.has_markup(mforms.LineMarkupBreakpoint, line):
            self.code_editor.remove_markup(mforms.LineMarkupBreakpoint, line)
            self._debug_printToOutput("Removed breakpoint line " + str(line+1))
            if line in self._listPosBreakpoints:
                self._listPosBreakpoints[line] = 0

        # Add markup of breakpoint in line
        else:
            self.code_editor.show_markup(mforms.LineMarkupBreakpoint, line)
            self._debug_printToOutput("Added breakpoint line " + str(line+1))
            if line in self._listPreBreakpoints:
                self._listPosBreakpoints[line] = self._listPreBreakpoints[line]

    # Get breakpoint position on SP and mark them in GUI editor
    # Only called once
    def _searchAndSetBreakpointOnGUI(self):
        script = "CALL common_schema.rdebug_show_routine('{0}', '{1}')".format(
            self.current_sqlEditor.defaultSchema, self.strp_name)

        text_pattern = r'(\[:(\d+)\])'
        regex_pattern = re.compile(text_pattern, re.IGNORECASE)

        # Temp variables to sync line and breakpoint IDs
        line_index = 0
        line_content = ''

        try:
            # Using grt executeQuery to prevent MySQL Error 2014
            result = grt.root.wb.sqlEditors[0].executeQuery(script, 0)
            #  = self.debuggerExecuteSingleQuery(script)
            if result:
                while result.nextRow():
                    line_content = result.stringFieldValue(0)
                    regex_result = regex_pattern.findall(line_content)
                    if regex_result:
                        self._listPreBreakpoints[line_index] = ''.join(
                            [x[1] for x in regex_result])
                        self._setBreakpointOnGUI(line_index)
                    line_index += 1
                result = None
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

        regex_pattern = r'(\w+);;(\w+);;(\w+)'
        pattern = re.compile(regex_pattern, re.IGNORECASE)

        try:
            self._workerThread = ThreadPool()
            async_result = self._workerThread.apply_async(
                self._appendParametersToList, args=(_list_parameters,))
            _list_parameters = async_result.get()
        except:
            raise

        if _list_parameters:
            top = 0
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
                    self.configs['debug_status'] = 'run'
                    self._execute_sp(dict_params, dict_ParamIn, dict_ParamOut)
            except:
                self.configs['debug_status'] = 'stop'
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise
        else:
            try:
                self.configs['debug_status'] = 'run'
                self._execute_sp(False, False, False)
            except:
                self.configs['debug_status'] = 'stop'
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise

    def _appendParametersToList(self, _list_parameters):
        params = ''
        routine_type = 'PROCEDURE'
        majorNumberVersion = self.current_sqlEditor.serverVersion.majorNumber
        minorNumberVersion = self.current_sqlEditor.serverVersion.minorNumber

        # Information_schema.parameters was introduced in MySQL 5.5
        # If MySQL < 5.5 we need to parse mysql.proc param_list instead
        if majorNumberVersion == 5 and  minorNumberVersion < 5:
            script_parameters = """SELECT REPLACE(param_list, ' ', ';;') AS params
                                    FROM mysql.proc p
                                    WHERE 1=1
                                    AND TYPE = '{0}'
                                    AND db = '{1}'
                                    AND specific_name = '{2}';""".format(
                routine_type.upper(),
                self.current_sqlEditor.defaultSchema,
                self.strp_name)

        else:
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
                if majorNumberVersion == 5 and minorNumberVersion < 5:
                    while result_parameters.nextRow():
                        tmp_params = result_parameters.stringByName('params')
                        tmp_params = tmp_params.split(',')
                        for p in tmp_params:
                            if p[:2] == ";;":
                                _list_parameters.append(p[2:])
                            else:
                                _list_parameters.append(p)
                else:
                    while result_parameters.nextRow():
                        params = ''.join([result_parameters.stringByName('parameter_mode'),
                                        ";;", result_parameters.stringByName(
                            'parameter_name'),
                            ";;", result_parameters.stringByName('data_type')])
                        _list_parameters.append(params)

                log_info("|| MajorNumber -> {0} - MinorNumber -> {1} || _list_params: ".format(majorNumberVersion, minorNumberVersion) + str(_list_parameters))
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

        if not _list_parameters or b_any("in;;" in st for st in _list_parameters):
            return _list_parameters
        else:
            return _list_parameters

    # Only called to start debug process for first time
    # After run once, has to be ignored
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
            res = self.workerExecuteMultiResultQuery(script_params_out)

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

        script += ')'

        async_result = self.rdebug_real_run()
        self.worker_async_result = self._workerThread.apply_async(
            self.workerExecuteMultiResultQuery, args=(script, False))

        self.printToOutput('Debug started! Status Changed.')
        try:
            if not params:
                result_list_debugger = async_result.get()
            else:
                time.sleep(0.2)
                result_list_debugger = async_result.get()

            if result_list_debugger:
                self._printFormattedText(
                    result_list_debugger, "debugger", True)

        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    def inputVariablesForm(self, ivf):
        if self.configs['debug_status'] == 'run':
            mainForm_wvariables = mforms.Form(None, mforms.FormToolWindow)
            mainForm_wvariables.set_title(
                'Set a value to a variable in ({0})'.format(self.strp_name))
            box_wvariables = mforms.newBox(False)
            box_wvariables.set_spacing(8)
            box_wvariables.set_padding(8)

            table_wvariable = mforms.newTable()
            table_wvariable.set_padding(10)
            table_wvariable.set_row_spacing(8)
            table_wvariable.set_column_spacing(2)
            table_wvariable.set_column_count(3)
            table_wvariable.set_row_count(3)

            top = 0
            bottom = 1

            label_variable_desc = mforms.newLabel(
                "Input a parameter or user-defined variable")
            label_variable_desc.set_style(mforms.InfoCaptionStyle)
            table_wvariable.add(label_variable_desc, 0, 2, top, bottom, 0)

            label_variable_name = mforms.newLabel("Variable:")
            table_wvariable.add(
                label_variable_name, 0, 1, top+1, bottom+1, 0)

            textbox_variable_name = mforms.newTextEntry(mforms.NormalEntry)
            textbox_variable_name.set_size(125, 20)
            table_wvariable.add(
                textbox_variable_name, 1, 2, top+1, bottom+1, 0)

            label_value_name = mforms.newLabel("Value:")
            table_wvariable.add(
                label_value_name, 0, 1, top+2, bottom+2, 0)

            textbox_value_name = mforms.newTextEntry(mforms.NormalEntry)
            textbox_value_name.set_size(125, 20)
            table_wvariable.add(
                textbox_value_name, 1, 2, top+2, bottom+2, 0)

            box_wvariables.add(table_wvariable, False, False)
            btn_variable_ok = mforms.newButton()
            btn_variable_ok.set_text("OK")
            btn_variable_cancel = mforms.newButton()
            btn_variable_cancel.set_text("Cancel")

            mforms.Utilities.add_end_ok_cancel_buttons(
                box_wvariables, btn_variable_ok, btn_variable_cancel)
            mainForm_wvariables.set_content(box_wvariables)
            mainForm_wvariables.center()

            try:
                if mainForm_wvariables.run_modal(btn_variable_ok, btn_variable_cancel):
                    self.rdebug_set_variables(
                        textbox_variable_name.get_string_value(), textbox_value_name.get_string_value())
            except:
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise
        else:
            mforms.Utilities.show_message(
                "Debug not running!", "You need to start debugging session before change value of a variable!", "OK", "", "")
            log_info("... Debug not running yet, clicked in step into")

        # rdebug_set_variables
    """
        Connections with MySQL Workbench API.
    """

    def setWorkerConnectionID(self):
        try:
            script = "SELECT CONNECTION_ID()"
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
            script = "SELECT CONNECTION_ID()"
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
            info = grt.root.wb.sqlEditors[0].connection
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
            info = grt.root.wb.sqlEditors[0].connection
            self.debugger_connection = MySQLConnection(info)
            self.debugger_connection.connect()
            if self.debugger_connection.is_connected:
                self.setDebuggerConnectionID()
            else:
                mforms.Utilities.show_warning(
                    "Connection Failed!", "Debugger session failed to connect!", "OK", "", "")
        except:
            raise

    # Return a MySQLResult object
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

    # Return a LIST of MySQLResult object
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

    # Used only for intern queries, preventing 'Commands Out Of Sync'
    def _watchdogConnection(self):
        try:
            info = grt.root.wb.sqlEditors[0].connection
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

    def _watchdogExecuteSingleQuery(self, script):
        try:
            result = self._watchdog_connection.executeQuery(script)
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
        self.rdebug_set_verbose(True)

    def removeCompiledDebug(self):
        self.rdebug_stop()
        time.sleep(0.3)
        self.compileDebugOnSp(False)
        if self.debugger_connection.is_connected:
            self.debugger_connection.disconnect()
        if self.worker_connection.is_connected:
            self.worker_connection.disconnect()
        if self._watchdog_connection.is_connected:
            self._watchdog_connection.disconnect()

        log_info('... Called remove debug from SP')

    def compileDebugOnSp(self, booleanAction):
        sch_name = self.current_sqlEditor.defaultSchema
        sp_name = self.strp_name
        params = "'{0}', '{1}', {2}".format(
            sch_name, sp_name, str(booleanAction))
        script = "CALL common_schema.rdebug_compile_routine({0})".format(
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
        script = "CALL common_schema.rdebug_start({0})".format(session_id)
        try:
            result = self.debuggerExecuteMultiResultQuery(script)
            if result:
                self._debug_printToOutput(
                    "rdebug has started! in {0}".format(session_id))
        except:
            mforms.Utilities.show_warning(
                "Error!", "rdebug failed to start at connection id {0} !".format(session_id), "", "CANCEL", "")
            raise

    def rdebug_stop(self, sst=''):
        script = "CALL common_schema.rdebug_stop()"
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result:
                self._debug_printToOutput("rdebug has stopped!")
                self.printToOutput("Debug has been canceled. Status changed.")
        except:
            mforms.Utilities.show_warning(
                "Error!", "Debug has failed to stop", "", "CANCEL", "")
            raise

    # Implicity call 'watch_variables',
    # 'show_current_statement' and 'stack_state' after each step
    def rdebug_set_verbose(self, boolean):
        script = 'CALL common_schema.rdebug_set_verbose({0})'.format(
            str(boolean))
        try:
            result = self.debuggerExecuteSingleQuery(script)
            result = None
        except:
            raise

    def rdebugStepInto(self, sst):
        if self.configs['debug_status'] == 'run':
            async_result = self._rdebugSetStep('into')
            try:
                time.sleep(0.3)
                result_list_debugger = async_result.get()
                if result_list_debugger:
                    self._printFormattedText(
                        result_list_debugger, "debugger", True)
            except:
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise
        else:
            mforms.Utilities.show_message(
                "Debug not running!", "You need to start debugging session before select a step", "OK", "", "")
            log_info("... Debug not running yet, clicked in step into")

    def rdebugStepOut(self, sst):
        if self.configs['debug_status'] == 'run':
            async_result = self._rdebugSetStep('out')
            try:
                time.sleep(0.3)
                result_list_debugger = async_result.get()
                if result_list_debugger:
                    self._printFormattedText(
                        result_list_debugger, "debugger", True)
            except:
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise
        else:
            mforms.Utilities.show_message(
                "Debug not running!", "You need to start debugging session before select a step", "OK", "", "")
            log_info("... Debug not running yet, clicked in step out")

    def rdebugStepOver(self, sst):
        if self.configs['debug_status'] == 'run':
            async_result = self._rdebugSetStep('over')
            try:
                time.sleep(0.3)
                result_list_debugger = async_result.get()
                if result_list_debugger:
                    self._printFormattedText(
                        result_list_debugger, "debugger", True)
            except:
                mforms.Utilities.show_warning(
                    "Error!", str(traceback.format_exc()), "OK", "", "")
                raise
        else:
            mforms.Utilities.show_message(
                "Debug not running!", "You need to start debugging session before select a step", "OK", "", "")
            log_info("... Debug not running yet, clicked in step over")

    def _rdebugSetStep(self, step):
        script = 'CALL common_schema.rdebug_step_{0}()'.format(step)
        try:
            res = self._debuggerThread.apply_async(
                self.debuggerExecuteMultiResultQuery, args=(script, False))
            if res:
                return res
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    # Return a ResultSet with debugging variables and values
    def _rdebug_watch_variables(self):
        script = "CALL common_schema.rdebug_watch_variables()"
        try:
            res = self.debuggerExecuteSingleQuery(script, False)
            if res:
                return res
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    def rdebug_set_variables(self, variable, new_value):
        script = "CALL common_schema.rdebug_set_variable('{0}', '{1}')".format(
            variable, new_value
        )

        try:
            res = self.debuggerExecuteSingleQuery(script, False)
            res = None
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    # Return current statement debugged
    def _rdebug_show_current_statement(self):
        script = "CALL common_schema.rdebug_show_statement()"
        try:
            res = self.debuggerExecuteSingleQuery(script, False)
            if res:
                return res
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    # add_bp: Boolean -> Add = True | Remove = False
    def _rdebug_set_breakpoint(self, position, add_bp):
        script_add = "call common_schema.rdebug_set_breakpoint('{0}', '{1}', {2}, null, {3})".format(
            self.current_sqlEditor.defaultSchema,
            self.strp_name,
            position,
            add_bp)
        try:
            result2 = self.debuggerExecuteSingleQuery(script_add, False)
            if result2:
                log_info("... Breakpoint added")

        except:
            mforms.Utilities.show_message(
                "Error", "Error when trying to add breakpoint!", "OK", "", "")
            raise

    # Start debugging processes
    def rdebug_run(self, rn):
        # 'Start' rdebug again if 'Stop' button has clicked
        # No implications if 'rdebug_start' is called twice
        self.rdebug_start(self.getWorkerConnectionID())

        # Remove all breakpoints before add then
        # If dessync between backend and gui occur
        if self._rdebugCheckBreakpoints():
            self._rdebugRemoveAllBreakpoints()

        # Add breakpoints setted on GUI
        for k, v in self._listPosBreakpoints.items():
            if v != 0:
                self._debuggerThread = ThreadPool(
                    processes=len(self._listPosBreakpoints))
                self._debuggerThread.apply_async(
                    self._rdebug_set_breakpoint, args=(v, True))

        # If breakpoints were not marked and setted,
        # Call last breakpoint to prevent freeze
        time.sleep(0.3)
        if not self._rdebugCheckBreakpoints():
            self._rdebugSetLastBreakpoint()

        self._inputParametersForm()

    def rdebug_real_run(self):
        script = "CALL common_schema.rdebug_run()"
        try:
            res = self._debuggerThread.apply_async(
                self.debuggerExecuteMultiResultQuery, args=(script, False))
            if res:
                log_info("rdebug_run called in {0}"
                         .format(self.getWorkerConnectionID))
                return res
        except:
            mforms.Utilities.show_warning(
                "Error!", str(traceback.format_exc()), "OK", "", "")
            raise

    # Set last breakpoint automatically
    # Only when no breakpoint was found on GUI
    # Prevent infinite loop on rdebug_run()
    def _rdebugSetLastBreakpoint(self):
        script = "SELECT statement_id FROM common_schema._rdebug_routine_statements WHERE routine_schema = '{0}' AND routine_name = '{1}' ORDER BY statement_id DESC LIMIT 1".format(
            self.current_sqlEditor.defaultSchema,
            self.strp_name)

        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result and result.nextRow():
                self._rdebug_set_breakpoint(
                    result.stringByName('statement_id'), True)
                log_info("... no breakpoints was found, set last one")
        except:
            mforms.Utilities.show_message(
                "Error", "Error calling breakpoint setter!", "OK", "", "")
            raise

    # Check all breakpoints setted via GUI, return a MySQLResultSet object
    def _rdebugCheckBreakpoints(self):
        script = "SELECT exists(select statement_id FROM common_schema._rdebug_breakpoint_hints WHERE worker_id = {0} AND routine_schema = '{1}' AND routine_name = '{2}')".format(
            self.getWorkerConnectionID(),
            self.current_sqlEditor.defaultSchema,
            self.strp_name
        )
        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result and result.nextRow():
                log_info("... checking breakpoints")
                return int(result.stringByIndex(1))
        except:
            self._debug_printToOutput("Failed to check breakpoints!")
            raise

    def _rdebugRemoveAllBreakpoints(self):
        script = "DELETE FROM common_schema._rdebug_breakpoint_hints WHERE worker_id = {0} AND routine_schema = '{1}' AND routine_name = '{2}'".format(
            self.getWorkerConnectionID(),
            self.current_sqlEditor.defaultSchema,
            self.strp_name
        )

        try:
            result = self.debuggerExecuteSingleQuery(script)
            if result:
                return True
        except:
            mforms.Utilities.show_message(
                "Error", "Error calling breakpoint setter!", "OK", "", "")
            raise

        return False

    def _checkWorkerWaiting(self):
        script = "SELECT IFNULL(MAX(state)='user sleep', true) as 'checkStatus' FROM information_schema.processlist WHERE id={0}".format(
            self.getWorkerConnectionID())
        res = self._watchdogExecuteSingleQuery(script)
        if res and res.nextRow():
            return int(res.stringByName('checkStatus'))
