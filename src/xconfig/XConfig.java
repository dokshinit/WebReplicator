/*
 * Copyright (c) 2013, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Реализация работы с конфигурационными файлами собственного формата.
 * <pre>
 * Синтаксис конфигурационного файла:
 * file.config := ELEMENT ...
 * ELEMENT := ATTR | NODE | ARRAY
 * ATTR := name="value"
 * NODE := name BLOCK
 * ARRAY := name[] { [BLOCK ...] }
 * BLOCK := { [ELEMENT ...] }
 *
 * LINECOMMENT := // comment to end of line
 * BLOCKCOMMENT := /&#x002A comment to end of comment block &#x002A/
 * </pre>
 * <p>
 * Весь файл состоит из последовательности элементов ELEMENT. Элементом может быть: атрибут, узел или массив. Аттрибут -
 * это конечное именованное значение. Узел - именованная совокупность элементов (иерархическая группа). Массив -
 * специальный случай реализации узла - содержит только узлы, которые выступают элементами массива (именование и доступ
 * - по индексу) и служебный атрибут "size", который отражает кол-во элементов в массиве. Массив определяется по имени
 * узла заканчивающегося на '[]'. Элементы и их атомарные части могут дополнительно разделяться символами пробела,
 * табуляции или перевода строки, а также комментариями. Значением атрибута является весь текст заключённый между
 * двойными кавычками (двойные кавычки в тексте значения должны быть экранированы комбинацией символов: обратный слэш +
 * кавычки).
 * <p>
 * Конфигурация может быть создана как на основе данных из текстового файла, так и из обычной текстовой строки - для
 * случая хранения конфигураций в БД в виде строк.
 * <p>
 * Конфиг может содержать узел-описатель с метаинформацией о конфиге для парсера. Синтаксис узла подпадает под общие
 * правила:
 * <pre>!CONFIG { version="1.0" locale="UTF-8" }</pre> Аттрибут 'version' - обязательный, 'locale' -
 * нет, если не задан - используется кодировка UTF-8. Этот узел может быть только первым элементом в конфиге иначе будет
 * выдана ошибка синтаксиса, он не содержится в корневом узле и в ключах! Данная возможность актуальна для конфигов в
 * файлах (которые могут быть созданы в разных системах и в разных кодировках).
 * <pre>
 * Пример массива:
 * array[] { {id="0" name="first"} {id="1" name="second"} {var="abc"} }
 * </pre>
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class XConfig {

    public static final String CONFIG_NODENAME = "!CONFIG";
    public static final String CONFIG_VERSIONNAME = "version";
    public static final String CONFIG_LOCALENAME = "locale";
    //
    public static final String DEFAULT_VERSION = "1.0";
    public static final String DEFAULT_LOCALE = "UTF-8";

    /**
     * Карта атрибутов (ключей).
     */
    private final HashMap<String, XAttribute> keymap;
    /**
     * Карта узлов.
     */
    private final HashMap<String, XNode> nodemap;
    /**
     * Корневой узел. Не имеет имени, создаётся автоматически и содержит все узлы и атрибуты.
     */
    private final XNode rootnode;
    //
    private String locale;
    private String version;

    // Текущие параметры парсинга.
    /**
     * Строка текущего сообщения об ошибке.
     */
    private String curErrorMessage;
    /**
     * Полный текст текущего сообщения об ошибке.
     */
    private String curErrorText;

    /**
     * Состояние.
     */
    private ParseState curState;
    /**
     * Родительская нода (блок которой парсится).
     */
    private XNode curNode;
    /**
     * Текущее распарсенное имя (ноды\атрибута\массива).
     */
    private String curName;
    /**
     * Текущее распарсенное значение (только для атрибута).
     */
    private String curValue;
    /**
     * Флаг выставляется после парсинга символа при возникнвении события.
     */
    private boolean isNodeCreated, isNodeClosed, isAttrCreated;
    private XNode createdNode, closedNode;
    private XAttribute createdAttr;
    //
    private XNode configNode;
    private ConfigNodeParseState configNodeState;

    // Текущие параметры обработки конфига:
    private String curString; // Текущая строка.
    private int curLine; // Номер текущей стоки.
    private int curPos; // Номер текущей позиции в текущей строке.
    private int curChar; // Текущий символ.
    private int prevChar; // Предыдущий символ (для первого символа = 0).

    /**
     * Конструктор.
     */
    public XConfig() {
        keymap = new HashMap<>();
        nodemap = new HashMap<>();
        rootnode = new XNode(this);
    }

    /**
     * Получение корневого узла.
     *
     * @return Корневой узел.
     */
    public XNode getRoot() {
        return rootnode;
    }

    /**
     * Получение текущей версии.
     *
     * @return Версия.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Получение текущей кодировки.
     *
     * @return Кодировка.
     */
    public String getLocale() {
        return locale;
    }

    /**
     * Проверка на существование конфигурационного узла.
     *
     * @return true - существует, false - нет.
     */
    public boolean isConfigNodeExists() {
        return configNode != null;
    }

    /**
     * Задание конфигурационного узла вручную.
     *
     * @param version Версия.
     * @param locale  Кодировка.
     */
    public void setConfig(String version, String locale) {
        this.version = version;
        this.locale = locale.trim().toUpperCase();
        configNode = getRoot().createNode(CONFIG_NODENAME);
        configNode.createAttribute(CONFIG_VERSIONNAME, this.version);
        configNode.createAttribute(CONFIG_LOCALENAME, this.locale);
        configNode.remove(); // Удалем из корневого узла.
    }

    /**
     * Задание конфигурационного узла вручную с параметрами по умолчанию.
     */
    public void setConfig() {
        setConfig(DEFAULT_VERSION, DEFAULT_LOCALE);
    }

    /**
     * Получение строки текущего сообщения об ошибке.
     *
     * @return Строка текущего сообщения об ошибке.
     */
    public String getCurErrorMessage() {
        return curErrorMessage;
    }

    /**
     * Получение полного текста текущего сообщения об ошибке.
     *
     * @return Полный текст текущего сообщения об ошибке.
     */
    public String getCurErrorText() {
        return curErrorText;
    }

    /**
     * Получение текущего состояния парсера.
     *
     * @return Состояние парсера.
     */
    public ParseState getCurState() {
        return curState;
    }

    /**
     * Получение текущего узла (массив - как специальный вид узла).
     *
     * @return Текущий узел.
     */
    public XNode getCurNode() {
        return curNode;
    }

    /**
     * Получение текущего имени.
     *
     * @return Текущее имя.
     */
    public String getCurName() {
        return curName;
    }

    /**
     * Получение текущего значения (для атрибута).
     *
     * @return Текущее значение.
     */
    public String getCurValue() {
        return curValue;
    }

    /**
     * Получение текущей разбираемой строки парсера.
     *
     * @return Текущая строка парсинга.
     */
    public String getCurString() {
        return curString;
    }

    /**
     * Получение номера текущей линии разбираемого текста.
     *
     * @return Номер текущей линии рабираемого текста.
     */
    public int getCurLine() {
        return curLine;
    }

    /**
     * Получение номера позиции текущего разбираемого символа.
     *
     * @return Номер позиции разбираемого символа.
     */
    public int getCurPos() {
        return curPos;
    }

    /**
     * Получение кода текущего разбираемого символа.
     *
     * @return Код текущего символа.
     */
    public int getCurChar() {
        return curChar;
    }

    /**
     * Получение кода предыдущего разобранного символа. Если не было еще разбора = 0.
     *
     * @return Код предыдущего символа.
     */
    public int getPrevChar() {
        return prevChar;
    }

    /**
     * Проверка символа: Можно использовать в именах?
     *
     * @param ch Символ.
     * @return Результат: true-можно, false-нет.
     */
    private boolean isAllowForName(int ch) {
        if (ch >= 'a' && ch <= 'z') {
            return true;
        }
        if (ch >= 'A' && ch <= 'Z') {
            return true;
        }
        if (ch >= '0' && ch <= '9') {
            return true;
        }
        if (ch == '_' || ch == '-' || ch == '!') {
            return true;
        }
        if (ch == '[' || ch == ']') {
            return true;
        }
        return false;
    }

    /**
     * Проверка символа: Разделитель слов строке? (внутри строки)
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isSpace(int ch) {
        return ch == ' ' || ch == '\t';
    }

    /**
     * Проверка символа: Разделитель строк? (перевод строки)
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isLineEnd(int ch) {
        return ch == 0xD || ch == 0xA;
    }

    /**
     * Проверка символа: Обратный слэш?
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isBackSlash(int ch) {
        return ch == '\\';
    }

    /**
     * Проверка символа: Открывающий блок? (открывающая фигурная скобка)
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isOpenBlock(int ch) {
        return ch == '{';
    }

    /**
     * Проверка символа: Закрывающий блок? (закрывающая фигурная скобка)
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isCloseBlock(int ch) {
        return ch == '}';
    }

    /**
     * Проверка символа: Знак равно для атрибута?
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isAttrEqual(int ch) {
        return ch == '=';
    }

    /**
     * Проверка символа: Начало\конец значения атрибута?
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isAttrQuote(int ch) {
        return ch == '"';
    }

    /**
     * Проверка символа: Слэш?
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isSlash(int ch) {
        return ch == '/';
    }

    /**
     * Проверка символа: Звездочка?
     *
     * @param ch Символ.
     * @return Результат: true-да, false-нет.
     */
    private boolean isStar(int ch) {
        return ch == '*';
    }

    /**
     * Формирование текстового отладочного сообщения о ошибке.
     *
     * @param msgtype Тип ошибки (блок, где возникла).
     * @param msg     Сообщение.
     */
    private void formErrorMessage(String msgtype, String msg) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < curPos; i++) {
            sb.append(" ");
        }
        curErrorMessage = msg;
        curErrorText = String.format(
                "%s: %s\n"
                        + " - INFO: Line=%d Pos=%d State=%s Node='%s'\n"
                        + " - TEXT: %s\n"
                        + "        %s|^|",
                msgtype, msg, curLine, curPos, curState.name(),
                curNode.getID(), curString, sb.toString());
    }

    /**
     * Вывод сообщения об ошибке на консоль и вызов исключения.
     *
     * @param msg Сообщение.
     * @throws ParseErrorException
     */
    private void parseErr(String msg) throws ParseErrorException {
        formErrorMessage("PARSE ERROR", msg);
        System.out.println(curErrorText);
        throw new ParseErrorException(curErrorMessage);
    }

    /**
     * Вывод сообщения об ошибке на консоль и вызов исключения, в случае если текущий узел является массивом.
     */
    private void parseErrIfArray() throws ParseErrorException {
        if (curNode.isArray()) {
            parseErr("Syntax error: Wrong state for array node!");
        }
    }

    /**
     * Состояния парсера.
     */
    public static enum ParseState {

        WAITNAME, // Ожидание имени.
        NAME, // Режим ввода имени.
        WAITEQUALOROPENBLOCK, // Ожидание имени или знака присвоения.
        WAITOPENQUOTE, // Ожидание кавычек для начала значения.
        VALUE, // Значение.
        WAITOPENBLOCK, // Ожидание открытия блока.
        COMMENT, // Обнаружен первый символ коментария.
        LINECOMMENT, // Коментарий-линия.
        BLOCKCOMMENT, // Коментарий-блок.
    }

    /**
     * Сосотояние обработки конфигурационного блока при парсинге.
     */
    private static enum ConfigNodeParseState {

        NOTOPENED, OPENED, FINISHED
    }

    /**
     * Парсинг текущего символа конфигурационнго файла.
     */
    private void parseChar() throws ParseErrorException {

        // Очищаем предыдущие флаги событий.
        createdNode = closedNode = null;
        createdAttr = null;
        isNodeCreated = isAttrCreated = false;

        switch (curState) {
            case WAITNAME: // Ожидание начала имени атрибута\ноды.
                parseErrIfArray();
                if (isSpace(curChar) || isLineEnd(curChar)) { // Пустые символы.
                    return; // Ожидаем дальше.
                }
                if (isAllowForName(curChar)) {
                    curState = ParseState.NAME;
                    curName = String.valueOf((char) curChar);
                    if (curChar == '[' || curChar == ']') {
                        parseErr("Syntax error: Array name must have at least one symbol!");
                    }
                    return;
                }
                if (isCloseBlock(curChar)) { // Завершение блока ноды.
                    isNodeClosed = true;
                    closedNode = curNode;
                    curNode = curNode.getParent();
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                    return;
                }
                if (isSlash(curChar)) {
                    curState = ParseState.COMMENT;
                    return;
                }
                parseErr("Syntax error: Wrong symbol!");

            case NAME: // Формирование имени атрибута\ноды.
                parseErrIfArray();
                if (isAllowForName(curChar)) {
                    curName += (char) curChar;
                    if (curChar == ']') {
                        if (!curName.endsWith("[]")) {
                            parseErr("Syntax error: Array name must end with '[]'!");
                        }
                        curState = ParseState.WAITOPENBLOCK; // Ожидаем блок массива.
                    }
                    return;
                } else {
                    curState = ParseState.WAITEQUALOROPENBLOCK;
                    parseChar();
                }
                return;

            case WAITEQUALOROPENBLOCK:
                parseErrIfArray();
                if (isSpace(curChar) || isLineEnd(curChar)) { // Пустые символы.
                    return; // Ожидаем дальше.
                }
                if (isAttrEqual(curChar)) {
                    curState = ParseState.WAITOPENQUOTE;
                    return;
                }
                if (isOpenBlock(curChar)) {
                    createdNode = curNode = curNode.createNode(curName);
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                    curName = "";
                    curValue = "";
                    isNodeCreated = true;
                    return;
                }
                if (isSlash(curChar)) {
                    curState = ParseState.COMMENT;
                    return;
                }
                parseErr("Syntax error: Wrong symbol!");

            case WAITOPENQUOTE: // Ожидание начала значения атрибута.
                parseErrIfArray();
                if (isSpace(curChar) || isLineEnd(curChar)) { // Пустые символы.
                    return; // Ожидаем дальше.
                }
                if (isAttrQuote(curChar)) {
                    curState = ParseState.VALUE;
                    return;
                }
                if (isSlash(curChar)) {
                    curState = ParseState.COMMENT;
                    return;
                }
                parseErr("Syntax error: Wrong symbol, wait open quote!");

            case VALUE: // Формирование значения атрибута.
                parseErrIfArray();
                if (isAttrQuote(curChar)) {
                    if (isBackSlash(prevChar)) {
                        // Замещаем обратный слэш на кавычки.
                        curValue = curValue.substring(0, curValue.length() - 1) + ((char) curChar);
                        return;
                    } else {
                        // Завершение формирования значения и добавление атрибута.
                        createdAttr = curNode.createAttribute(curName, curValue);
                        curState = ParseState.WAITNAME;
                        curName = "";
                        curValue = "";
                        isAttrCreated = true;
                        return;
                    }
                }
                if (isLineEnd(curChar)) {
                    parseErr("Syntax error: Missing end quote!");
                }
                curValue += (char) curChar;
                return;

            case WAITOPENBLOCK: // Ожидание начала блока.
                if (isSpace(curChar) || isLineEnd(curChar)) { // Пустые символы.
                    return; // Ожидаем дальше.
                }
                if (isOpenBlock(curChar)) {
                    if (curNode.isArray()) {
                        curName = "[" + curNode.getNodes().size() + "]";
                    }
                    curNode = curNode.createNode(curName);
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                    curName = "";
                    curValue = "";
                    return;
                }
                if (curNode.isArray() && isCloseBlock(curChar)) { // Завершение блока массива.
                    curNode = curNode.getParent();
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                    return;
                }
                if (isSlash(curChar)) {
                    curState = ParseState.COMMENT;
                    return;
                }
                parseErr("Syntax error: Wrong symbol!");

            case COMMENT:
                if (isSlash(curChar)) { // Комментарий-строка.
                    curState = ParseState.LINECOMMENT;
                    return;
                }
                if (isStar(curChar)) { // Комментарй-блок.
                    curState = ParseState.BLOCKCOMMENT;
                    return;
                }
                parseErr("Syntax error: Wrong symbol!");

            case LINECOMMENT:
                if (isLineEnd(curChar)) {
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                }
                return;

            case BLOCKCOMMENT:
                if (isSlash(curChar) && isStar(prevChar)) {
                    curState = curNode.isArray() ? ParseState.WAITOPENBLOCK : ParseState.WAITNAME;
                }
                return;

            default:
                parseErr("Syntax error: Wrong state!");
        }
    }

    /**
     * Очистка конфига от всех данных и сброс состояния к первоначальному.
     */
    public void clear() {
        keymap.clear();
        nodemap.clear();
        rootnode.clear();

        locale = DEFAULT_LOCALE;
        version = DEFAULT_VERSION;
        configNode = null;
        configNodeState = null;

        curErrorMessage = null;
        curErrorText = null;
        curState = null;
        curNode = null;
        curName = "";
        curValue = "";

        isNodeCreated = isNodeClosed = isAttrCreated = false;
        createdNode = closedNode = null;
        createdAttr = null;
    }

    /**
     * Вывод сообщения об ошибке на консоль и вызов исключения.
     *
     * @param msg Сообщение.
     */
    private void configNodeErr(String msg) throws ConfigNodeParseException {
        formErrorMessage("CONFIG ERROR", msg);
        System.out.println(curErrorText);
        throw new ConfigNodeParseException(curErrorMessage);
    }

    /**
     * Загрузка конфигурации из потока.
     *
     * @param stream  Поток.
     * @param slocale
     * @throws IOException
     * @throws xconfig.ParseErrorException
     * @throws xconfig.ConfigNodeParseException
     */
    public void load(InputStream stream, String slocale) throws IOException, ParseErrorException, ConfigNodeParseException {

        clear();

        locale = (slocale == null || slocale.isEmpty()) ? DEFAULT_LOCALE : slocale.toUpperCase();
        Charset charset = Charset.forName(locale);
        // Не используем промежуточный буферизированный поток, т.к. ридер сам реализует буферизацию кэшем.
        InputStreamFlexDecodeReader reader = new InputStreamFlexDecodeReader(stream, charset.newDecoder());

        curLine = curPos = 0;
        curState = ParseState.WAITNAME;
        curNode = rootnode;
        curName = curValue = curString = "";
        prevChar = 0;
        configNode = null;
        configNodeState = ConfigNodeParseState.NOTOPENED;
        boolean isbreak = false;

        while (!isbreak) {
            prevChar = curChar;
            curChar = reader.read();
            if (curChar == -1) {
                // При завершении искусственно добавляем виртуальный перевод строки,
                // чтобы отрабатывался как разделитель!
                curChar = '\n';
                isbreak = true;
            }
            if (!isLineEnd(curChar)) {
                curString += (char) curChar; // Дополняем текущую строку, если это не перевод строки.
            }
            parseChar();
            if (configNodeState != ConfigNodeParseState.FINISHED) {
                checkConfigNodeParsing();
                if (configNodeState == ConfigNodeParseState.FINISHED) {

                    Charset newcharset = Charset.forName(locale.toUpperCase());
                    if (!newcharset.equals(charset)) {
                        charset = newcharset;
                        reader.setDecoder(newcharset.newDecoder());
                    }
                }
            }
            // Актуализация текущих параметров.
            if (isLineEnd(curChar)) {
                if (curChar == '\n') { // '\r' игнорируем!
                    curLine++;
                    curPos = 0;
                    curString = "";
                }
            } else {
                curPos++;
            }
        }
    }

    /**
     * Проверка парсинга конфигурационного узла.
     *
     * @return Флаг успешного выполнения: true - ОК, false - ошибка.
     */
    private void checkConfigNodeParsing() throws ConfigNodeParseException {
        // Проверка на корректность конфигурационного узла.
        // Узел !CONFIG должен начинаться с первого символа в файле и
        // обязательно содержать атрибут version. Кодировку символов
        // может не содержать - тогда по умолчанию utf-8.
        switch (configNodeState) {

            case NOTOPENED: // Если узел конфига не открыт.
                if (isNodeCreated) {
                    if (CONFIG_NODENAME.equals(createdNode.getName())) {
                        if (createdNode.getParent() != rootnode) {
                            configNodeErr("Config is not in rootnode!"); // Если конфиг не в руте - ошибка!
                        }
                        if (rootnode.getNodeCount() != 1 || rootnode.getAttributeCount() != 0) {
                            configNodeErr("Config is not first element!"); // Если конфиг не первый элемент в руте - ошибка!
                        }
                        configNodeState = ConfigNodeParseState.OPENED; // Если создан узел !CONFIG - выставляем флаг.
                        configNode = createdNode;
                    }
                }
                break;

            case OPENED: // Если узел конфига открыт и не завершен.
                if (isNodeCreated) { // Вложенные узлы запрещены.
                    configNodeErr("Config can not include subnodes!");
                }
                if (isAttrCreated) { // Аттрибуты только разрешенные.
                    if (!CONFIG_VERSIONNAME.equals(createdAttr.getName())
                            && !CONFIG_LOCALENAME.equals(createdAttr.getName())) {
                        configNodeErr("Config can not have varies attr!");
                    }
                }
                if (isNodeClosed && CONFIG_NODENAME.equals(closedNode.getName())) {
                    version = configNode.getKey(CONFIG_VERSIONNAME, null);
                    if (version == null) {
                        configNodeErr("Config must have version attr!");
                    }
                    locale = configNode.getKey(CONFIG_LOCALENAME, locale).toUpperCase();
                    configNodeState = ConfigNodeParseState.FINISHED;
                    configNode.remove();
                }
                break;
        }
    }

    public void load(InputStream stream) throws IOException, ParseErrorException, ConfigNodeParseException {
        load(stream, null);
    }

    /**
     * Загрузка конфиагурации из файла.
     *
     * @param filename Имя файла.
     * @throws java.io.IOException
     * @throws xconfig.ParseErrorException
     * @throws xconfig.ConfigNodeParseException
     */
    public void load(String filename) throws IOException, ParseErrorException, ConfigNodeParseException {
        load(new FileInputStream(filename));
    }

    /**
     * Загрузка конфигурации из текста в виде строки.
     *
     * @param text Текст.
     * @throws java.io.IOException
     * @throws xconfig.ParseErrorException
     * @throws xconfig.ConfigNodeParseException
     */
    public void loadFromString(String text) throws IOException, ParseErrorException, ConfigNodeParseException {
        load(new ByteArrayInputStream(text.getBytes(DEFAULT_LOCALE)), DEFAULT_LOCALE);
    }

    /**
     * Режим форматирования при преобразовании конфигурации в текст.
     */
    public static enum FormatMode {

        FILE,
        STRING,
        COMPACTSTRING
    }

    ;

    /**
     * Возвращает текст для конфигурационного узла. Если конфигурационный узел не задан - возращает пустое значение.
     *
     * @return Текст конфигурационного узла, если не задан - возвращает null.
     */
    public String configNodeToText() {
        if (configNode != null) {
            StringBuilder sb = new StringBuilder();
            nodeToText(sb, "", configNode, FormatMode.STRING);
            return sb.toString();
        } else {
            return null;
        }
    }

    /**
     * Преобразование узла в текст.
     *
     * @param sb     Билдер строк для возврата результата.
     * @param prefix Префикс для форматирования отступа.
     * @param node   Узел.
     * @param mode   Режим форматирования.
     */
    private void nodeToText(StringBuilder sb, String prefix, XNode node, FormatMode mode) {
        boolean isroot = node.isRoot();
        boolean isarrayelement = !isroot && node.getParent().isArray();

        String br, sp;
        switch (mode) {
            default:
            case FILE:
                br = "\n";
                sp = " ";
                break;
            case STRING:
                br = sp = " ";
                break;
            case COMPACTSTRING:
                br = sp = "";
                break;
        }
        String newprefix = prefix;
        if (isroot) {
            // Для корневого узла при файловом выводе - вставляем первым узлом конфиг, если он задан.
            if (mode == FormatMode.FILE && isConfigNodeExists()) {
                sb.append(configNodeToText()).append("\n\n");
            }
        } else {
            if (isarrayelement) {
                sb.append(prefix).append("{").append(br);
            } else {
                sb.append(prefix).append(node.getName())
                        .append(node.isArray() ? ("[]" + sp + "{") : (sp + "{")).append(br);
            }
            if (mode == FormatMode.FILE) {
                newprefix += "\t";
            }
        }
        if (!node.isArray()) {
            for (XAttribute a : node.getAttributes()) {
                sb.append(newprefix).append(a.getName()).append("=\"")
                        .append(a.getMaskedValue()).append("\"").append(br);
            }
        }
        for (XNode n : node.getNodes()) {
            nodeToText(sb, newprefix, n, mode);
        }
        if (!isroot) {
            sb.append(prefix).append("}").append(br);
        }
    }

    /**
     * Сохранение конфигурации в строку.
     *
     * @param mode Режим форматирования.
     * @return Текст конфигурации.
     */
    public String saveToString(FormatMode mode) {
        StringBuilder sb = new StringBuilder();
        if (configNode != null && mode == FormatMode.FILE) {
            sb.append(String.format("%s { version=\"%s\" locale=\"%s\" }\n\n", CONFIG_NODENAME, version, locale));
        }
        nodeToText(sb, "", rootnode, mode);
        return sb.toString();
    }

    /**
     * Сохранение конфигурации в поток.
     *
     * @param stream Поток.
     * @param mode   Режим форматирования.
     */
    public void save(PrintStream stream, FormatMode mode) {
        stream.print(saveToString(mode));
        stream.flush();
    }

    /**
     * Сохранение конфигурации в файл.
     *
     * @param filename Имя файла.
     * @throws FileNotFoundException
     * @throws java.io.UnsupportedEncodingException
     */
    public void save(final String filename) throws FileNotFoundException, UnsupportedEncodingException {
        save(new PrintStream(filename, DEFAULT_LOCALE), FormatMode.FILE);
    }

    /**
     * Вывод в поток всех элементов из карты узлов и карты ключей. Для контроля и отладки.
     *
     * @param out Поток.
     */
    public void outAllKeys(PrintStream out) {
        keymap.entrySet().stream().forEach((e) -> {
            XAttribute a = e.getValue();
            out.println("key=" + e.getKey() + " [" + a.getName() + " = " + a.getValue()
                    + "] (path=" + a.getPath() + " id=" + a.getID() + ")");
        });
        nodemap.entrySet().stream().forEach((e) -> {
            XNode n = e.getValue();
            out.println("node=" + e.getKey() + " [" + n.getName() + "][" + n.getPath()
                    + "] (path=" + n.getPath()
                    + " cattrpath=" + n.getChildAttributePath()
                    + " cnonepath=" + n.getChildNodePath()
                    + " id=" + n.getID() + " cid=" + n.getChildID("")
                    + " isArray=" + n.isArray() + ")");
        });
    }

    // Для добавления\удаления элеметов в картах.
    void putAttributeKey(String key, XAttribute attr) {
        keymap.put(key, attr);
    }

    void removeAttrKey(String key) {
        keymap.remove(key);
    }

    void putNodeKey(String key, XNode node) {
        nodemap.put(key, node);
    }

    void removeNodeKey(String key) {
        nodemap.remove(key);
    }

    /**
     * Получение атрибута для указанного ключа.
     *
     * @param key Ключ.
     * @return Аттрибут.
     */
    public XAttribute getAttr(final String key) {
        return keymap.get(key);
    }

    /**
     * Получение узла для указанного ключа.
     *
     * @param key Ключ.
     * @return Узел. Если такого узла нет - null.
     */
    public XNode getNode(final String key) {
        return nodemap.get(key);
    }

    /**
     * Установка значения атрибута для указанного ключа.
     *
     * @param key   Ключ.
     * @param value Значение атрибута.
     * @throws xconfig.KeyNotFoundException
     */
    public void setKey(final String key, final String value) throws KeyNotFoundException {
        XAttribute attr = getAttr(key);
        if (attr == null) {
            throw new KeyNotFoundException("Key [" + key + "]: not found!");
        }
        attr.setValue(value);
    }

    /**
     * Получение значения атрибута для указанного ключа.
     *
     * @param key Ключ.
     * @return Значение ключа.
     * @throws xconfig.KeyNotFoundException
     */
    public String getKey(final String key) throws KeyNotFoundException {
        XAttribute attr = getAttr(key);
        if (attr == null) {
            throw new KeyNotFoundException("Key [" + key + "]: not found!");
        }
        return attr.getValue();
    }

    /**
     * Получение значения атрибута для указанного ключа.
     *
     * @param key          Ключ.
     * @param defaultvalue Значение возвращаемое при отсутствии ключа.
     * @return Значение ключа или значение по умолчанию.
     */
    public String getKey(final String key, final String defaultvalue) {
        XAttribute attr = getAttr(key);
        return attr == null ? defaultvalue : attr.getValue();
    }

    /**
     * Получение значения атрибута для указанного ключа в виде целого числа (подразумевается, что атрибут существует и
     * содержит корректное значение).
     *
     * @param key Ключ.
     * @return Значение ключа.
     * @throws xconfig.KeyNotFoundException
     * @throws xconfig.WrongKeyValueException
     */
    public int getIntKey(final String key) throws KeyNotFoundException, WrongKeyValueException {
        XAttribute attr = getAttr(key);
        if (attr == null) {
            throw new KeyNotFoundException("Key [" + key + "]: not found!");
        }
        return attr.getValueAsInt();
    }

    /**
     * Получение значения атрибута для указанного ключа в виде целого числа (подразумевается, что атрибут существует и
     * содержит корректное значение). Если ключ не существует, то возврат значения по умолчанию.
     *
     * @param key          Ключ.
     * @param defaultvalue Значение возвращаемое при отсутствии ключа.
     * @return Значение ключа или значение по умолчанию.
     * @throws xconfig.WrongKeyValueException
     */
    public int getIntKey(final String key, Integer defaultvalue) throws WrongKeyValueException {
        try {
            return getIntKey(key);
        } catch (KeyNotFoundException ex) {
            return defaultvalue;
        }
    }

    public Date getDateKey(final String key, DateFormat fmt) throws KeyNotFoundException, WrongKeyValueException {
        XAttribute attr = getAttr(key);
        if (attr == null) {
            throw new KeyNotFoundException("Key [" + key + "]: not found!");
        }
        return attr.getValueAsDate(fmt);
    }

    public Date getDateKey(final String key, DateFormat fmt, Date defaultvalue) throws WrongKeyValueException {
        try {
            return getDateKey(key, fmt);
        } catch (KeyNotFoundException ex) {
            return defaultvalue;
        }
    }

    public BigDecimal getDecimalKey(final String key) throws KeyNotFoundException, WrongKeyValueException {
        XAttribute attr = getAttr(key);
        if (attr == null) {
            throw new KeyNotFoundException("Key [" + key + "]: not found!");
        }
        return attr.getValueAsDecimal();
    }

    public BigDecimal getDecimalKey(final String key, BigDecimal defaultvalue) throws WrongKeyValueException {
        try {
            return getDecimalKey(key);
        } catch (KeyNotFoundException ex) {
            return defaultvalue;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  ТЕСТЫ
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws IOException, ParseErrorException, ConfigNodeParseException {
        XConfig config = new XConfig();
        config.load("app.config");
        //config.loadFromString("id=\"1\" class=\"FDTopazDevice\" port=\"/dev/ttyUSB0\" bitrate=\"4800\" databits=\"7\" stopbits=\"2\" parity=\"EVEN\"");
        System.out.println("===FILE===");
        config.save(System.out, FormatMode.FILE);
        System.out.println();
        System.out.println("===STRING===");
        config.save(System.out, FormatMode.STRING);
        System.out.println();
        System.out.println("===COMPACTSTRING===");
        config.save(System.out, FormatMode.COMPACTSTRING);
        System.out.println();
        System.out.println("===KEYMAP===");
        config.outAllKeys(System.out);
    }
}
