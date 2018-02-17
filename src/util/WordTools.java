/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class WordTools {

    private final static int MAX_TRIPLE_POWER = 6;

    private static final int one = 0;
    private static final int four = 1;
    private static final int many = 2;

    private final static String[][] power = new String[][]{
        {"", "", ""}, // 1
        {"тысяча", "тысячи", "тысяч"}, // 2 female!
        {"миллион", "миллиона", "миллионов"}, // 3
        {"миллиард", "миллиарда", "миллиардов"}, // 4
        {"триллион", "триллиона", "триллионов"}, // 5
        {"квадриллион", "квадриллиона", "квадриллионов"}, // 6
        {"квинтиллион", "квинтиллиона", "квинтиллионов"} // 7
    };

    private static final int male = 0;
    private static final int female = 1;
    private static final int dec10 = 2;
    private static final int dec20 = 3;
    private static final int hun = 4;

    private final static String[][] digit = new String[][]{
        {"", "", "десять", "", ""},
        {"один", "одна", "одиннадцать", "десять", "сто"},
        {"два", "две", "двенадцать", "двадцать", "двести"},
        {"три", "три", "тринадцать", "тридцать", "триста"},
        {"четыре", "четыре", "четырнадцать", "сорок", "четыреста"},
        {"пять", "пять", "пятнадцать", "пятьдесят", "пятьсот"},
        {"шесть", "шесть", "шестнадцать", "шестьдесят", "шестьсот"},
        {"семь", "семь", "семнадцать", "семьдесят", "семьсот"},
        {"восемь", "восемь", "восемнадцать", "восемьдесят", "восемьсот"},
        {"девять", "девять", "девятнадцать", "девяносто", "девятьсот"}
    };

    // Индекс в массиве power для числа-индекса.
    private final static int[] link = {
        many, one, four, four, four, many, many, many, many, many,
        many, many, many, many, many, many, many, many, many, many
    };

    private final static String[][] sign = new String[][]{
        {"рубль", "рубля", "рублей"},
        {"копейка", "копейки", "копеек"}
    };

    /**
     * Формирование строки целого числа прописью. Все буквы в нижнем регистре.
     *
     * @param sum Число.
     * @return Строка числа прописью.
     */
    public static String toWords(long sum) {

        if (sum == 0) {
            return "ноль";
        }

        StringBuilder sb = new StringBuilder();
        long divisor; //делитель

        if (sum < 0) {
            sb.append("минус");
            sum = -sum;
        }

        int i, mny;
        for (i = 0, divisor = 1; i < MAX_TRIPLE_POWER; i++) {
            divisor *= 1000;
        }

        for (i = MAX_TRIPLE_POWER - 1; i >= 0; i--) {
            divisor /= 1000;
            mny = (int) (sum / divisor);
            sum %= divisor;
            //System.out.println("i=" + i + " mny=" + mny);
            if (mny == 0) {
                if (i == 0) {
                    sb.append(" ").append(power[i][one]);
                }
            } else {
                if (mny >= 100) { // Сотни.
                    sb.append(" ").append(digit[mny / 100][hun]);
                    mny %= 100;
                }
                if (mny >= 20) { // Десятки.
                    sb.append(" ").append(digit[mny / 10][dec20]);
                    mny %= 10;
                }
                if (mny >= 10) { // Второй десяток.
                    sb.append(" ").append(digit[mny - 10][dec10]);
                    //mny %= 10;
                } else {
                    if (mny >= 1) {
                        sb.append(" ").append(digit[mny][i == 1 ? female : male]);
                    }
                }
                //System.out.println("onemny=" + mny);
                sb.append(" ").append(power[i][link[mny]]);
            }
        }
        return sb.toString().trim();
    }

    /**
     * Формирование строки денежной суммы прописью. По умолчанию копейки выводятся цифрами.
     *
     * @param sum Денежная сумма.
     * @return Строка денежной суммы прописью.
     */
    public static String toMoneyWords(BigDecimal sum) {
        return toMoneyWords(sum, false);
    }

    /**
     * Формирование строки денежной суммы прописью. Первая буква заглавная. У суммы учитываются
     * только два знака после запятой (остальные отбрасываются - не округляются!).
     *
     * @param sum        Денежная сумма.
     * @param isKopWords Режим вывода копеек: true - прописью, false - цифрами.
     * @return Строка денежной суммы прописью.
     */
    public static String toMoneyWords(BigDecimal sum, boolean isKopWords) {
        long sumint = sum.longValue();
        long sumfrac = sum.remainder(BigDecimal.ONE).scaleByPowerOfTen(2).longValue(); // ровно два знака после запятой!
        // Генерируем сумму рублей.
        String sint = toWords(sumint);
        String sfrac = isKopWords ? toWords(sumfrac) : String.format("%02d", sumfrac);
        // Находим названия мер.
        int ir = (int) (sumint % 100);
        ir = ir >= 20 ? ir % 10 : ir;
        int ik = (int) (sumfrac % 100);
        ik = ik >= 20 ? ik % 10 : ik;
        String srub = sign[0][link[ir]];
        String skop = sign[1][link[ik]];
        // Делаем первую букву заглавной.
        sint = sint.substring(0, 1).toUpperCase() + sint.substring(1);

        return sint + " " + srub + " " + sfrac + " " + skop;
    }

    private static void show(long value) {
        System.out.println("V=" + value + " STR=" + toWords(value));
    }

    private static void showRUB(double value) {
        System.out.println("V=" + value + " STR=" + toMoneyWords(new BigDecimal(value).setScale(2, RoundingMode.HALF_UP)));
    }

    public static void main(String[] args) {
        show(1234567890);
        show(123456789);
        show(12345678);
        show(1234567);
        show(123456);
        show(12345);
        show(1234);

        showRUB(1234567890.08);
        showRUB(123456789.58);
        showRUB(12345678.94);
        showRUB(1234567.21);
        showRUB(123456.15);
        showRUB(12345.12);
        showRUB(1234.01);

        showRUB(229912.27);

//        BigDecimal b = new BigDecimal("10.999");
//        long v = b.longValue();
//        long v2 = b.remainder(BigDecimal.ONE).scaleByPowerOfTen(b.scale()).longValue();
//        System.out.println("V=" + v + " V2=" + v2);
    }
}
