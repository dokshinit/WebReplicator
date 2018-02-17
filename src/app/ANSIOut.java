package app;

import util.NumberTools;
import util.StringTools;

/**
 * @author Aleksey Dokshin <dant.it@gmail.com> (14.02.18).
 */
public class ANSIOut {

    public static class Colors {
        public int color, bgColor;
        public boolean isBold, isItalic, isUnderline;

        public Colors(int color, int bgColor, boolean isBold, boolean isItalic, boolean isUnderline) {
            this.color = color;
            this.bgColor = bgColor;
            this.isBold = isBold;
            this.isItalic = isItalic;
            this.isUnderline = isUnderline;
        }

        public Colors() {
            this(7, 0, false, false, false);
        }

        public Colors(Colors src) {
            copyFrom(src);
        }

        public void copyFrom(Colors src) {
            color = src.color;
            bgColor = src.bgColor;
            isBold = src.isBold;
            isItalic = src.isItalic;
            isUnderline = src.isUnderline;
        }
    }

    public final char ESC = 0x1B;
    public final String CSI = "" + ESC + '[';

    private Colors curColors, backColors;

    public ANSIOut() {
        curColors = new Colors();
        backColors = new Colors();
    }

    public static int getWidth() {
        return NumberTools.parseIntSafe(System.getenv("COLUMNS"), 0);
    }

    public static int getHeight() {
        return NumberTools.parseIntSafe(System.getenv("LINES"), 0);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public String escClear() {
        return CSI + "2J";
    }

    public String escAt(int x, int y) {
        return CSI + String.format("%d;%d", y, x) + "H"; // f
    }

    public String escAtX(int x) {
        return CSI + String.format("%d", x) + "G";
    }

    public String escUp(int y) {
        return CSI + String.format("%d", y) + "A";
    }

    public String escEraseLine(int x) {
        return CSI + String.format("%d", x) + "K";
    }

    public String escBackupPos() {
        return CSI + "s";
    }

    public String escRestorePos() {
        return CSI + "u";
    }

    public String escCursorOff() {
        return CSI + "?25l";
    }

    public String escCursorOn() {
        return CSI + "?25h";
    }

    public String escColor8(int c8) {
        return escAttr(30 + c8);
    }

    public String escColor256(int c256) {
        return escAttr(38, 5, c256);
    }

    public String escBgColor8(int c8) {
        return escAttr(40 + c8);
    }

    public String escBgColor256(int c256) {
        return escAttr(48, 5, c256);
    }

    /**
     * Установка текущих аттрибутов.
     * <pre>
     * Стандартные аттрибуты:
     *   Действия: 0-Reset all attributes, 1-Bright, 2-Dim, 4-Underscore, 5-Blink, 7-Reverse, 8-Hidden;
     *   Цвет текста: 30 + {цвет};
     *   Цвет фона: 40 + {цвет};
     *   где {цвет} - один из: 0-Black, 1-Red, 2-Green, 3-Yellow, 4-Blue, 5-Magenta, 6-Cyan, 7-White.]
     * </pre>
     *
     * @param attrs
     */
    public String escAttr(int... attrs) {
        String s = "";
        boolean isfirst = true;
        for (int i : attrs) {
            if (!isfirst) s += ";";
            s += i;
            isfirst = false;
        }
        return CSI + s + "m";
    }

    public String escBold() {
        return escAttr(1);
    }

    public String escBoldOff() {
        return escAttr(21);
    }

    public String escBold(boolean is) {
        return is ? escBold() : escBoldOff();
    }

    public String escItalic() {
        return escAttr(3);
    }

    public String escItalicOff() {
        return escAttr(23);
    }

    public String escItalic(boolean is) {
        return is ? escItalic() : escItalicOff();
    }

    public String escUnderline() {
        return escAttr(4);
    }

    public String escUnderlineOff() {
        return escAttr(24);
    }

    public String escUnderline(boolean is) {
        return is ? escUnderline() : escUnderlineOff();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Если width задано - работает корректно только если в тексте отсутствуют ANSI коды!
    public ANSIOut print(int x, int width, String fmt, Object... params) {
        String str;
        if (params.length == 0) {
            str = fmt;
        } else {
            str = String.format(fmt, params);
        }
        if (width > 0) { // Ограничиваем или расширяем до указанной ширины.
            System.out.print(escAtX(x) + StringTools.fill(' ', width) + escAtX(x));
//            int len = str.length();
//            if (len > width) {
//                str = str.substring(0, width);
//            } else {
//                str = str + StringTools.fill(' ', width - len);
//            }
        }
        System.out.print(str);
        return this;
    }

    public ANSIOut print(int width, String fmt, Object... params) {
        return print(0, width, fmt, params);
    }

    public ANSIOut print(String fmt, Object... params) {
        return print(0, fmt, params);
    }

    public ANSIOut println(int x, int width, String fmt, Object... params) {
        return print(x, width, fmt, params).print("\n");
    }

    public ANSIOut println(int width, String fmt, Object... params) {
        return print(0, width, fmt, params).print("\n");
    }

    public ANSIOut println(String fmt, Object... params) {
        return println(0, fmt, params);
    }

    public ANSIOut println(int x, int width) {
        return println(x, width, "");
    }

    public ANSIOut println(int width) {
        return println(0, width);
    }

    public ANSIOut println() {
        return println("");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public ANSIOut clear() {
        return print(escClear());
    }

    // Не работает!
    public ANSIOut backupPos() {
        return print(escBackupPos());
    }

    // Не работает!
    public ANSIOut restorePos() {
        return print(escRestorePos());
    }

    public ANSIOut at(int x, int y) {
        return print(escAt(x, y));
    }

    public ANSIOut atX(int x) {
        return print(escAtX(x));
    }

    public ANSIOut attr(int... attrs) {
        return print(escAttr(attrs));
    }

    public ANSIOut reset() {
        print(escAttr(0)).color(curColors.color, curColors.bgColor);
        if (curColors.isBold) bold();
        if (curColors.isItalic) italic();
        if (curColors.isUnderline) underline();
        return this;
    }

    public ANSIOut bold() {
        curColors.isBold = true;
        return print(escBold());
    }

    public ANSIOut boldOff() {
        curColors.isBold = false;
        return print(escBoldOff());
    }

    public ANSIOut bold(boolean is) {
        return curColors.isBold ? bold() : boldOff();
    }

    public ANSIOut italic(boolean is) {
        return curColors.isItalic ? italic() : italicOff();
    }

    public ANSIOut italic() {
        curColors.isItalic = true;
        return print(escItalic());
    }

    public ANSIOut italicOff() {
        curColors.isItalic = false;
        return print(escItalicOff());
    }

    public ANSIOut underline(boolean is) {
        return curColors.isUnderline ? underline() : underlineOff();
    }

    public ANSIOut underline() {
        curColors.isUnderline = true;
        return print(escUnderline());
    }

    public ANSIOut underlineOff() {
        curColors.isUnderline = false;
        return print(escUnderlineOff());
    }


    public ANSIOut color(int c256) {
        curColors.color = c256;
        return print(escColor256(c256));
    }

    public ANSIOut bgcolor(int c256) {
        curColors.bgColor = c256;
        return print(escBgColor256(c256));
    }

    public ANSIOut color(int c256, int bg256) {
        return color(c256).bgcolor(bg256);
    }

    public ANSIOut colors(Colors src) {
        if (curColors.color != src.color) color(src.color);
        if (curColors.bgColor != src.bgColor) bgcolor(src.bgColor);
        if (curColors.isBold != src.isBold) bold(src.isBold);
        if (curColors.isItalic != src.isItalic) italic(src.isItalic);
        if (curColors.isUnderline != src.isUnderline) underline(src.isUnderline);
        return this;
    }

    public ANSIOut colorsForce(Colors src) {
        return color(src.color, src.bgColor).bold(src.isBold).italic(src.isItalic).underline(src.isUnderline);
    }

    public ANSIOut backupColors() {
        backColors.copyFrom(curColors);
        return this;
    }

    public ANSIOut restoreColors() {
        curColors.copyFrom(backColors);
        return this;
    }

    public Colors getColors() {
        return new Colors(curColors);
    }

    public ANSIOut setColors(Colors src) {
        curColors.copyFrom(src);
        return this;
    }

    public int c() {
        return curColors.color;
    }

    public int bg() {
        return curColors.bgColor;
    }

    public Colors getBackColors() {
        return new Colors(backColors);
    }

    public ANSIOut cursorOff() {
        return print(escCursorOff());
    }

    public ANSIOut cursorOn() {
        return print(escCursorOn());
    }
}
