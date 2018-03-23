using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;

namespace FileCompressor
{
    class GZipCompressor
    {
        int additionalThreadCount;
        bool compressInProgress = false;
        bool decompressInProgress = false;
        long blockSize = 1024 * 1024;//1mb
        int bufferSize = 100;//максимальное количество блоков во входном и выходном буфере
        PerformanceCounter memCounter;

        object locker = new object();//блокирующий обьект для разделяемых ресурсов
        public GZipCompressor()
        {
            additionalThreadCount = Environment.ProcessorCount;
            if (additionalThreadCount > 1)
            {
                additionalThreadCount -= 1;//количесво потоков - 1 для чтения,и записи в файлы
            }
            memCounter = new PerformanceCounter("Memory", "Available MBytes");
        }

        public void Compress(string sourceFile, string compressedFile)
        {
            //проверяем доступное кол-во памяти
            if (!IsEnoughMemoryCheck())
            {
                return;
            }
            //вместо коллекций можно использовать структуры данных чтобы облегчить работу GC
            Queue<DataBlock> readDataBuffer = new Queue<DataBlock>();//входная очередь
            List<DataBlock> compressedDataBuffer = new List<DataBlock>();//выходная очередь

            int id = 0; //id блока данных для выходного потока
            int currentIdToWrite = 0; //id нужного блока для записи в файл
            compressInProgress = true;
            long totalBlocksAmount; //общее количесво блоков
            long nextTimeToInfoPrint = 0;
            long timeBetweenInfoPrint = 1000;//1s время между отоброжением информации о процессе в консоль

            Process currentProcess = Process.GetCurrentProcess();
            Stopwatch watch = new Stopwatch();
            watch.Start();
            //создаем файловые потоке для входного и выходного файла
            using (FileStream outputStream = File.Create(compressedFile))
            using (FileStream sourceStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read))
            {
                long fileLength = sourceStream.Length;// длинна исходного файла

                //проверяем доступное место на диске
                if (!IsEnoughSpaceCheck(compressedFile, fileLength))
                {
                    return;
                }

                long blocksCount = fileLength / (long)blockSize;
                if (fileLength % blockSize > 0)
                {
                    blocksCount += 1;
                }

                blocksCount += 2;//два дополнительных блока для информации о всем файле

                totalBlocksAmount = blocksCount;
                byte[] size = BitConverter.GetBytes(fileLength);
                compressedDataBuffer.Add(new DataBlock(size, id++)); //записываем размер несжатого файла в выходной поток, id = 0;

                size = BitConverter.GetBytes(totalBlocksAmount);
                compressedDataBuffer.Add(new DataBlock(size, id++)); //записываем общее количесво блоков в файле
                size = null;

                for (int i = 0; i < additionalThreadCount; i++)//создаем потоки обработки 
                {
                    Thread thread = new Thread(new ParameterizedThreadStart(CompressBlockAsynchThread));
                    thread.Start(new BuffersReferences(readDataBuffer, compressedDataBuffer));
                }//если один процессор то один дополнительный поток для обработки, но в однопроцессорной среде можно добавить метод который будет пережимать в текущем потоке без буферов

                while (compressInProgress)
                {
                    //блок чтения файла
                    if (fileLength > 0 && readDataBuffer.Count < bufferSize)//максимальный размер буфера
                    {
                        if (fileLength < blockSize)//длинна остатка от файла меньше размера блока
                        {
                            blockSize = fileLength;
                        }
                        fileLength -= blockSize;

                        byte[] block = new byte[blockSize];
                        int byteReaded = sourceStream.Read(block, 0, block.Length);//читаем блок из файла
                        lock (locker)//блокируем входной буфер
                        {
                            readDataBuffer.Enqueue(new DataBlock(block, id++));//вставляем блок и id блока в буфер чтения
                        }
                        block = null;
                    }


                    //блок записи файла 
                    if (compressedDataBuffer.Count > 0)//если в буфере есть блок то читаем
                    {
                        byte[] compressedBlock = new byte[0];
                        lock (locker)
                        {
                            if (compressedDataBuffer.Count > 0)
                            {
                                foreach (DataBlock block in compressedDataBuffer)
                                {
                                    if (block.Id == currentIdToWrite)//находим нужный по порядку блок
                                    {
                                        compressedBlock = block.Data;//читаем его и удаляем уз буфера
                                        compressedDataBuffer.Remove(block);
                                        break;
                                    }
                                }
                            }
                        }

                        if (compressedBlock.Length > 0)//блок был прочтен
                        {
                            GC.Collect();//вызываем сборщик мусора чтобы убрал за мусором коллекции compressedDataBuffer
                            blocksCount--;//сколько еще осталось блоков
                            currentIdToWrite++;//следующий нужный блок
                            outputStream.Write(compressedBlock, 0, compressedBlock.Length);
                            compressedBlock = null;
                        }
                        compressedBlock = null;
                    }

                    //вывод информации о процессе
                    if (watch.ElapsedMilliseconds > nextTimeToInfoPrint)
                    {
                        Console.WriteLine("ReadDataBuffer count: " + readDataBuffer.Count);
                        Console.WriteLine("CompressedDataBuffer count: " + compressedDataBuffer.Count);
                        Console.WriteLine("Progress: " + ((1f - (float)blocksCount / totalBlocksAmount)) * 100 + "%");
                        Console.WriteLine("Time elasped: " + watch.Elapsed.ToString());
                        currentProcess.Refresh();
                        Console.WriteLine("Memory used: " + (currentProcess.WorkingSet64 / (1024 * 1024)) + " MB");
                        nextTimeToInfoPrint += timeBetweenInfoPrint;
                    }

                    if (blocksCount == 0)//все блоки записаны
                    {
                        compressInProgress = false;
                    }

                }
                watch.Stop();
                Console.WriteLine("File Compresed, Time To Compress: " + watch.Elapsed.ToString());
            }
            readDataBuffer = null;
            compressedDataBuffer = null;
            GC.Collect();
        }

        private void CompressBlockAsynchThread(object buffersReferences)
        {
            BuffersReferences buffers = (BuffersReferences)buffersReferences;
            Queue<DataBlock> readDataBuffer = buffers.InputDataBuffer;
            List<DataBlock> compressedDataBuffer = buffers.OutputDataBuffer;

            while (compressInProgress)
            {
                while (compressedDataBuffer.Count >= bufferSize)//выходной буфер заполнен
                {
                    Console.WriteLine("Thread wait for space in Output buffer");
                    Thread.Sleep(1000);//Ждем освобождения буфера
                }
                if (readDataBuffer.Count > 0)
                {
                    DataBlock readedBlock = new DataBlock(new byte[0], 0);
                    bool blockReaded = false;
                    lock (locker)
                    {
                        if (readDataBuffer.Count > 0)
                        {
                            readedBlock = readDataBuffer.Dequeue();
                            blockReaded = true;
                        }
                    }
                    if (blockReaded)
                    {
                        GC.Collect();
                        byte[] size;
                        long blockSize;
                        using (MemoryStream compressedDataFull = new MemoryStream())
                        using (MemoryStream compressedDataStream = new MemoryStream())
                        {
                            using (GZipStream streamCompressed = new GZipStream(compressedDataStream, CompressionMode.Compress))
                            {
                                blockSize = readedBlock.Data.Length;
                                streamCompressed.Write(readedBlock.Data, 0, readedBlock.Data.Length);
                            }

                            byte[] compressedData = compressedDataStream.ToArray();

                            size = BitConverter.GetBytes(blockSize);//пишем размер блока файла
                            compressedDataFull.Write(size, 0, size.Length);

                            size = BitConverter.GetBytes((long)compressedData.Length);//размер сжатого блока
                            compressedDataFull.Write(size, 0, size.Length);
                            size = null;

                            compressedDataFull.Write(compressedData, 0, compressedData.Length);//сжатый блок
                            compressedData = null;

                            lock (locker)
                            {
                                compressedDataBuffer.Add(new DataBlock(compressedDataFull.ToArray(), readedBlock.Id));
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Thread Wait for Data");
                    Thread.Sleep(1000);//wait to readDataBuffer enqueue
                }
            }
        }

        public void Decompress(string compressedFile, string decompressedFile)
        {
            //проверяем доступное кол-во памяти
            if (!IsEnoughMemoryCheck())
            {
                return;
            }

            //вместо коллекций можно использовать структуры данных чтобы облегчить работу GC
            Queue<DataBlock> readDataBuffer = new Queue<DataBlock>();//входная очередь
            List<DataBlock> decompressedDataBuffer = new List<DataBlock>();//выходная очередь

            int id = 0; //id блока данных для выходного потока
            int currentIdToWrite = 0; //id нужного блока для зваписи в файл
            decompressInProgress = true;
            //long totalBlocksAmount; //общее количесво блоков
            long nextTimeToInfoPrint = 0;
            long timeBetweenInfoPrint = 1000;//1s время между отоброжением информации о процессе в консоль
            bool isSingleThread = (additionalThreadCount == 0);// однопоточный процесс

            Process currentProcess = Process.GetCurrentProcess();
            Stopwatch watch = new Stopwatch();
            watch.Start();
            //создаем файловые потоке для входного и выходного файла
            using (FileStream outputStream = File.Create(decompressedFile))
            using (FileStream sourceStream = new FileStream(compressedFile, FileMode.Open, FileAccess.Read))
            {
                byte[] size = new byte[sizeof(long)];
                sourceStream.Read(size, 0, size.Length);
                long restFileLength = BitConverter.ToInt64(size, 0);// считываем длинну исходного файла
                long uncompressedFullFileLength = restFileLength;
                size = null;

                if (uncompressedFullFileLength < 0)
                {
                    throw new InvalidDataException("Source File Corrupted");
                }

                //проверяем доступное место на диске
                if (!IsEnoughSpaceCheck(decompressedFile, uncompressedFullFileLength))
                {
                    return;
                }

                size = new byte[sizeof(long)];
                sourceStream.Read(size, 0, size.Length);
                long totalBlocksAmount = (BitConverter.ToInt64(size, 0)) - 2;// считываем общее количесвто блоков минус два блока которые прочитали

                long blockSize = 0;
                long compressedBlockSize = 0;

                for (int i = 0; i < additionalThreadCount; i++)//создаем потоки обработки 
                {
                    Thread thread = new Thread(new ParameterizedThreadStart(DecompressBlockAsynchThread));
                    thread.Start(new BuffersReferences(readDataBuffer, decompressedDataBuffer));
                }
                //один дополнительный поток для обработки, но в однопроцессорной среде можно добавить метод который будет пережимать в текущем потоке без буферов

                while (decompressInProgress)
                {
                    //блок чтения файла
                    if (restFileLength > 0 && readDataBuffer.Count < bufferSize)//максимальный размер буфера
                    {
                        size = new byte[sizeof(long)];
                        sourceStream.Read(size, 0, size.Length);
                        blockSize = BitConverter.ToInt64(size, 0);//размер блока

                        restFileLength -= blockSize;

                        size = new byte[sizeof(long)];
                        sourceStream.Read(size, 0, size.Length);
                        compressedBlockSize = BitConverter.ToInt64(size, 0);//размер сжатого блока
                        size = null;

                        //проверяем на возможные ошибки
                        long workingSet = Process.GetCurrentProcess().WorkingSet64; //выделенная память для процесса
                        if (blockSize > uncompressedFullFileLength || blockSize > workingSet)
                        {
                            throw new InvalidDataException("Source File Corrupted");//длинна блока больше всего файла
                            //или длинна блока больше выделенной памяти для процесса
                        }

                        if (compressedBlockSize > uncompressedFullFileLength || compressedBlockSize > workingSet)
                        {
                            throw new InvalidDataException("Source File Corrupted");//длинна сжатого блока больше всего файла 
                            //или сжатый блок больше выделенной памяти
                        }

                        byte[] block = new byte[compressedBlockSize];
                        sourceStream.Read(block, 0, block.Length);//читаем блок из файла

                        lock (locker)//блокируем входной буфер
                        {
                            readDataBuffer.Enqueue(new DataBlock(block, id++, blockSize));//вставляем блок, id и размер несжатого блока в буфер чтения
                        }
                        block = null;
                    }

                    //блок записи в файл
                    if (decompressedDataBuffer.Count > 0)//если в буфере есть блок то читаем
                    {
                        byte[] compressedBlock = new byte[0];
                        lock (locker)
                        {
                            if (decompressedDataBuffer.Count > 0)
                            {
                                foreach (DataBlock block in decompressedDataBuffer)
                                {
                                    if (block.Id == currentIdToWrite)//находим нужный по порядку блок
                                    {
                                        compressedBlock = block.Data;//читаем его и удаляем уз буфера
                                        decompressedDataBuffer.Remove(block);
                                        break;
                                    }
                                }
                            }
                        }
                        if (compressedBlock.Length > 0)//блок был прочтен
                        {
                            GC.Collect();//вызываем сборщик мусора чтобы убрал мусором коллекции decompressedDataBuffer
                            currentIdToWrite++;//следующий нужный блок
                            outputStream.Write(compressedBlock, 0, compressedBlock.Length);
                            compressedBlock = null;
                            totalBlocksAmount--;
                        }
                        compressedBlock = null;
                    }

                    //информация о процессе
                    if (watch.ElapsedMilliseconds > nextTimeToInfoPrint)
                    {
                        Console.WriteLine("ReadDataBuffer count: " + readDataBuffer.Count);
                        Console.WriteLine("CompressedDataBuffer count: " + decompressedDataBuffer.Count);
                        Console.WriteLine("Progress: " + ((1f - (float)restFileLength / uncompressedFullFileLength)) * 100 + "%");
                        Console.WriteLine("Time elasped: " + watch.Elapsed.ToString());
                        currentProcess.Refresh();
                        Console.WriteLine("Memory used: " + (currentProcess.WorkingSet64 / (1024 * 1024)) + " MB");
                        nextTimeToInfoPrint += timeBetweenInfoPrint;
                    }

                    if (totalBlocksAmount == 0)//все блоки записаны
                    {
                        decompressInProgress = false;
                    }
                }
                watch.Stop();
                Console.WriteLine("File Decompresed, Time To Decompress: " + watch.Elapsed.ToString());
            }
            readDataBuffer = null;
            decompressedDataBuffer = null;
            GC.Collect();
        }

        private void DecompressBlockAsynchThread(object buffersReferences)
        {
            BuffersReferences buffers = (BuffersReferences)buffersReferences;
            Queue<DataBlock> readDataBuffer = buffers.InputDataBuffer;
            List<DataBlock> decompressedDataBuffer = buffers.OutputDataBuffer;

            while (decompressInProgress)
            {
                while (decompressedDataBuffer.Count >= bufferSize)//выходной буфер заполнен
                {
                    Console.WriteLine("Thread wait for space in Output buffer");
                    Thread.Sleep(100);//Ждем освобождения буфера
                }
                if (readDataBuffer.Count > 0)
                {
                    DataBlock readedBlock = new DataBlock(new byte[0], 0, 0);
                    bool blockReaded = false;
                    lock (locker)
                    {
                        if (readDataBuffer.Count > 0)
                        {
                            readedBlock = readDataBuffer.Dequeue();
                            blockReaded = true;
                        }
                    }
                    if (blockReaded)
                    {
                        GC.Collect();
                        using (MemoryStream compressedDataStream = new MemoryStream(readedBlock.Data))
                        {
                            byte[] uncompressedData = new byte[readedBlock.DecompressedBlockSize];

                            using (GZipStream streamCompressed = new GZipStream(compressedDataStream, CompressionMode.Decompress))
                            {
                                streamCompressed.Read(uncompressedData, 0, uncompressedData.Length);
                            }

                            lock (locker)
                            {
                                decompressedDataBuffer.Add(new DataBlock(uncompressedData, readedBlock.Id));
                            }
                            uncompressedData = null;
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Thread Wait for Data");
                    Thread.Sleep(100);//wait to readDataBuffer enqueue
                }
            }
        }

        private bool IsEnoughMemoryCheck()
        {
            long availableMemoryMB = (long)memCounter.NextValue();
            long usedMemory = (long)(((blockSize * bufferSize * 2 + blockSize * 4 * additionalThreadCount) / (1024 * 1024)) * 1.1f);//приблизительное максимальное потребление памяти
            if (availableMemoryMB < usedMemory)
            {
                Console.WriteLine("Not Enough Available Memory");
                return false;
            }
            return true;
        }

        private bool IsEnoughSpaceCheck(string pathToFile, long fileLength)
        {
            string path = Path.GetPathRoot(pathToFile);
            if (path == "")
            {
                path = Path.GetPathRoot(Environment.CurrentDirectory);
            }
            DriveInfo driveInfo = new DriveInfo(path);
            long availableFreeSpace = driveInfo.AvailableFreeSpace;

            if (fileLength > availableFreeSpace)
            {
                Console.WriteLine("Not enough free space or source file corrupted");
                return false;
            }
            return true;
        }

        class BuffersReferences
        {
            public Queue<DataBlock> InputDataBuffer { get; set; }
            public List<DataBlock> OutputDataBuffer { get; set; }

            public BuffersReferences(Queue<DataBlock> inputDataBuffer, List<DataBlock> outputDataBuffer)
            {
                InputDataBuffer = inputDataBuffer;
                OutputDataBuffer = outputDataBuffer;
            }
        }

        struct DataBlock
        {
            public byte[] Data { get; set; }
            public int Id { get; set; }
            public long DecompressedBlockSize { get; set; }

            public DataBlock(byte[] data, int id, long decompressedBlockSize = 0)
            {
                Data = data;
                Id = id;
                DecompressedBlockSize = decompressedBlockSize;
            }
        }
    }
}
