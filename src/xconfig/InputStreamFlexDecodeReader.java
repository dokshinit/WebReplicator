/*
 * Copyright (c) 2015, Aleksey Nikolaevich Dokshin. All right reserved.
 * Contacts: dant.it@gmail.com, dokshin@list.ru.
 */
package xconfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

/**
 * Ридер входного потока с возможностью произвольно переназначать декодер в процессе чтения данных. Для случаев, когда в
 * одном потоке смешаны данные в разных кодировках или есть необходимость смены кодировки по данным поступившим из
 * потока (как пример - данные о кодировке содержатся в начальных данных потока).
 *
 * @author Докшин Алексей Николаевич <dant.it@gmail.com>
 */
public class InputStreamFlexDecodeReader implements Closeable {

    /** Размер кэша по умолчанию. */
    public static final int DEFAULT_CACHE_SIZE = 8192;

    /** Входной поток (источник сырых данных). */
    private final InputStream stream;
    /** Текущий декодер для формирования символьных данных. */
    private CharsetDecoder decoder;

    /** Оперативный кэш данных источника. */
    private final byte[] arrayIn;
    /** Буфер для оперирования данными кэша источника. */
    private final ByteBuffer bbIn;
    /** Массив-приёмник для декодированных данных (посимвольный вывод). */
    private final char[] arrayOut;
    /** Буфер для оперирования данными массива-приёмника. */
    private final CharBuffer cbOut;
    /** Флаг достижения конца данных во входном потоке. */
    private boolean isEndOfStream;

    /**
     * Конструктор.
     *
     * @param inputstream Входной поток.
     * @param csdecoder   Декодер символов.
     * @param cachesize   Размер кэша.
     */
    public InputStreamFlexDecodeReader(InputStream inputstream, CharsetDecoder csdecoder, int cachesize) {
        // Если кэш не задан (<=0) то по умолчанию, если задан и меньше 128, то 128.
        cachesize = cachesize > 0 ? (cachesize < 128 ? 128 : cachesize) : DEFAULT_CACHE_SIZE;
        stream = inputstream;
        decoder = csdecoder != null ? csdecoder : Charset.defaultCharset().newDecoder();
        arrayIn = new byte[cachesize];
        bbIn = ByteBuffer.wrap(arrayIn);
        bbIn.position(0);
        bbIn.limit(0);
        arrayOut = new char[1];
        cbOut = CharBuffer.wrap(arrayOut);
        isEndOfStream = false;
    }

    /**
     * Конструктор.
     *
     * @param stream  Входной поток.
     * @param decoder Декодер символов.
     */
    public InputStreamFlexDecodeReader(InputStream stream, CharsetDecoder decoder) {
        this(stream, decoder, 0);
    }

    /**
     * Конструктор. Декодер создаётся для кодировки по умолчанию.
     *
     * @param stream Входной поток.
     */
    public InputStreamFlexDecodeReader(InputStream stream) {
        this(stream, null, 0);
    }

    /**
     * Актуализация кэша.
     *
     * @throws IOException
     */
    private void cache() throws IOException {
        // Если все данные считаны ранее, то в кэш заносить нечего.
        if (!isEndOfStream) {
            int pos = bbIn.position();
            int size = bbIn.limit();
            // Проверяем состояние кэша.
            if (size == arrayIn.length) { // Если массив заполнен до конца.
                // Максимальный размер символа = 6 байт, для корретного декодирования
                // следим, чтобы размер "остатка" был не менее 6 байт. Если меньше,
                // то переносим остаток в начало и догружаем кэш.
                if (pos < arrayIn.length - 6) { // Если остаток больше 6 байт, то обновлять кэш не нужно.
                    return;
                }
                // Позиция вышла за окно - передвигаем окно кэша дальше.
                size = arrayIn.length - pos;
                for (int i = 0; i < size; i++) {
                    arrayIn[i] = arrayIn[pos + i];
                }
                pos = 0;
            }
            // Массив заполнен не до конца - пробуем заполнить.
            int n = stream.read(arrayIn, size, arrayIn.length - size);
            if (n == -1) {
                isEndOfStream = true;
                n = 0;
            }
            bbIn.position(pos);
            bbIn.limit(size + n);
        }
    }

    /**
     * Чтение одного декодированного символа.
     *
     * @return Декодированный символ. Если -1 - достигнут конец потока, данных больше нет.
     * @throws IOException
     */
    public int read() throws IOException {

        // Актуализация кэша.
        cache();

        // Если все данные обработаны - достигнут конец.
        if (bbIn.position() >= bbIn.limit()) {
            return -1;
        }
        // Декодируем символ.
        cbOut.position(0);
        CoderResult cr;
        synchronized (this) {
            cr = decoder.decode(bbIn, cbOut, true);
        }
        if (cr.isError()) {
            return 0; // Может быть как-то иначе ошибки выкидывать?
        }
        return arrayOut[0];
    }

    /**
     * Получение текущей позиции в кэше.
     *
     * @return Текущая позиция в кэше.
     */
    public int cachePosition() {
        return bbIn.position();
    }

    /**
     * Получение текущего размера данных в кэше.
     *
     * @return Текущий размер данных в кэше.
     */
    public int cacheSize() {
        return bbIn.limit();
    }

    /**
     * Получение размера кэша.
     *
     * @return Размер кэша.
     */
    public int cacheMaxSize() {
        return arrayIn.length;
    }

    /**
     * Получение текущего декодера.
     *
     * @return Текущий декодер.
     */
    public synchronized CharsetDecoder getDecoder() {
        return decoder;
    }

    /**
     * Установка нового декодера.
     *
     * @param newdecoder Новый декодер.
     */
    public synchronized void setDecoder(CharsetDecoder newdecoder) {
        decoder = newdecoder;
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
