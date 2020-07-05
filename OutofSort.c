/*
前提条件：
・gensort（http://www.ordinal.com/gensort.html）にて作成されたデータをソートする
（ダウンロード先　
　windows・・・"gensort-win-1.5.zip"
  linux・・・"gensort-linux-1.5.tar.gz"）
・データの作成方法はgensort.exeを"gensort -a N ファイル名"を引数として実行する
（-aにてASCIIレコードを生成する．またNはレコードの個数を指定する．）
・ソートするキーは10byteのASCIIレコードにて行う
・使用するPCのCPUは12スレッド以上を搭載していること
（動作確認済み：intel corei7 8700）
*/

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdbool.h>
#include<time.h>
#include<Windows.h>
#include<process.h>
#include<direct.h>


#pragma warning(disable:4996)
#pragma comment(lib, "winmm.lib")

//SWAP関数　ソート時に使用します
#define SWAP(a, b) (tmp) = (a);(a) = (b);(b) = (tmp);
//クイックソートから挿入ソートに切り替える配列の長さ　今回は32
#define M 32
//スタックする配列の長さ
#define NSTACK 50
//一時ファイルに切り替えるレコードの個数
#define OutFileLimit 100000

//最大使用スレッド数を制限　当環境は6コア12スレッドなので12に指定
#define NUM_THREADS 12
//スレッドに割り振る仕事の最小値を指定　今回は1000
#define THRESHOLD 1000
//現在作動しているスレッド数
int workingThreads = 0;
//同期監視
CRITICAL_SECTION cs;

int Foldernumber = 1;
int Bool_1 = 0;

//関数のプロトタイプ宣言
void Outmerge(unsigned long i, unsigned long q);
void sort(unsigned long left, unsigned long right, char** arr);
void sortQuickandMerge(unsigned long left, unsigned long right, char** arr);

//マージソートを行うための構造体
struct Merge_info {
	unsigned long name;
	unsigned long fold;
};

//クイックソートを行うための構造体
struct thread_info {
	char** arr;
	unsigned long l;
	unsigned long r;
};

//スレッド起動時の関数　構造体を実質的な引数とする
unsigned __stdcall Out_merge_ThreadEntry(void* arg) {
	struct Merge_info* context = (struct Merge_info*)arg;
	Outmerge(context->name, context->fold);
	return 0;
}

//スレッド起動時の関数　構造体を実質的な引数とする
unsigned __stdcall qsortThreadEntry(void* arg) {
	struct thread_info* context = (struct thread_info*)arg;
	char** arr = context->arr;
	sort(context->l, context->r, arr);
	return 0;
}

//外部ソートにてマージソートを行う関数
void Outmerge(unsigned long n, unsigned long f) {
	unsigned long name = n;
	unsigned long Foldname = f;
	//ソートしたいレコードの長さ
	char merge_first[20];
	char merge_second[20];
	char merge_result[20];
	char Foldername[20];
	//マージする一時ファイル2つとマージされたファイル1つ
	FILE* mergeone;
	FILE* mergetwo;
	FILE* mergeresult;
	int count = 0;
	int checkleftcount = 0, checkrightcount = 0;
	int F1 = 0, F2 = 0;
	char* input_one = (char*)malloc(sizeof(char) * 100);
	char* input_two = (char*)malloc(sizeof(char) * 100);
	sprintf(merge_first, "dat%d\\%d.dat", Foldname, name);
	sprintf(merge_second, "dat%d\\%d.dat", Foldname, name + 1);
	sprintf(Foldername, "dat%d", Foldname + 1);
	if (_mkdir(Foldername) == 0) {
		printf("new folder\n");
	}
	sprintf(merge_result, "dat%d\\%d.dat", Foldname + 1, name / 2);
	if ((mergeone = fopen(merge_first, "r")) == NULL) {
		printf("file open error\n");
		return;
	}
	else if ((mergetwo = fopen(merge_second, "r")) == NULL) {
		mergeresult = fopen(merge_result, "w");
		while (fgets(input_one, 100, mergeone) != NULL) {
			fprintf(mergeresult, "%s", input_one);
		}
		printf("second file open error\n");
		fclose(mergeone);
		fclose(mergeresult);
		free(input_one);
		free(input_two);
		return;
	}
	mergeresult = fopen(merge_result, "w");
	fgets(input_one, 100, mergeone);
	fgets(input_two, 100, mergetwo);
	for (;;) {
		if (memcmp(input_one, input_two, 10) < 0) {
			fprintf(mergeresult, "%s", input_one);
			checkleftcount++;
			if (fgets(input_one, 100, mergeone) == NULL) {
				fprintf(mergeresult, "%s", input_two);
				checkrightcount++;
				while (fgets(input_two, 100, mergetwo) != NULL) {
					fprintf(mergeresult, "%s", input_two);
					checkrightcount++;
				}
				break;
				F1 = 1;
			}
		}
		else {
			fprintf(mergeresult, "%s", input_two);
			checkrightcount++;
			if (fgets(input_two, 100, mergetwo) == NULL) {
				fprintf(mergeresult, "%s", input_one);
				checkleftcount++;
				while (fgets(input_one, 100, mergeone) != NULL) {
					fprintf(mergeresult, "%s", input_one);
					checkleftcount++;
				}
				break;
				F2 = 1;
			}
		}
		if (F1 == 1 || F2 == 1) {
			break;
		}
	}
	printf("Left: %d  Right: %d\n", checkleftcount, checkrightcount);
	fclose(mergeone);
	fclose(mergetwo);
	fclose(mergeresult);
	free(input_one);
	free(input_two);
	return;
}

//内部ソートにてクイックソートを行う関数
void sort(unsigned long left, unsigned long right, char** arr) {
	unsigned long i, j, k;
	unsigned long l = left;
	unsigned long r = right;
	int* stack, nstack = 0;
	char* a, *tmp;

	//sortMT

	HANDLE aThread;
	bool childThread = false;
	bool permit = true;
	struct thread_info par;

	//sortMT end
	stack = (int*)malloc(NSTACK * sizeof(int)) - 1;
	for (;;) {
		if (r - l < M) {
			for (j = l + 1; j <= r; j++) {
				a = arr[j];
				for (i = j - 1; i >= l; i--) {
					if (memcmp(arr[i], a, 10) <= 0) {
						break;
					}
					arr[i + 1] = arr[i];
				}
				arr[i + 1] = a;
			}
			if (nstack == 0) {
				//sortMT

				if (childThread) {
					WaitForSingleObject(aThread, INFINITE);
					CloseHandle(aThread);
				}

				//sortMT end
				break;
			}
			r = stack[nstack--];
			l = stack[nstack--];
		}
		else {
			k = (l + r) >> 1;
			SWAP(arr[k], arr[l + 1])
				if (memcmp(arr[l + 1], arr[r], 10) > 0) {
					SWAP(arr[l + 1], arr[r])
				}
			if (memcmp(arr[l], arr[r], 10) > 0) {
				SWAP(arr[l], arr[r])
			}
			if (memcmp(arr[l + 1], arr[l], 10) > 0) {
				SWAP(arr[l + 1], arr[l])
			}
			i = l + 1;
			j = r;
			a = arr[l];
			for (;;) {
				do i++; while (memcmp(arr[i], a, 10) < 0);
				do j--; while (memcmp(arr[j], a, 10) > 0);
				if (j < i) {
					break;
				}
				SWAP(arr[i], arr[j])
			}
			arr[l] = arr[j];
			arr[j] = a;
			//sortMT
			if (!childThread && permit) {
				EnterCriticalSection(&cs);
				if (workingThreads < NUM_THREADS - 1) {
					workingThreads++;
				}
				else {
					permit = false;
				}
				LeaveCriticalSection(&cs);
			}
			if (!childThread && permit && (r - i + 1 > THRESHOLD || j - l > THRESHOLD)) {
				if (r - i + 1 >= j - l) {
					par.r = r;
					par.l = i;
					r = j - 1;
				}
				else {
					par.r = j - 1;
					par.l = l;
					l = i;
				}
				par.arr = arr;

				aThread = (HANDLE)_beginthreadex(
					NULL,
					0,
					&qsortThreadEntry,
					(void*)&par,
					0,
					NULL
				);
				if (aThread == NULL) {
					printf("_beginthreadex() failed, error: %d\n", GetLastError());
					return;
				}
				childThread = true;
			}
			else {
				//sortMT end
				nstack += 2;
				if (nstack > NSTACK) {
					printf("Error too small\n");
				}
				if (r - i + 1 >= j - 1) {
					stack[nstack] = r;
					stack[nstack - 1] = i;
					r = j - 1;
				}
				else {
					stack[nstack] = j - 1;
					stack[nstack - 1] = l;
					l = i;
				}
			}
		}
	}
	free(stack + 1);
}


void sortQuickandMerge(unsigned long left, unsigned long right, char** arr) {
	unsigned long l = left;
	unsigned long r = right;
	if (left >= right) {
		exit(0);
	}

	InitializeCriticalSection(&cs);
	sort(l, r, arr);
	DeleteCriticalSection(&cs);
	return;
}


int main() {
	//ソートしたいfile名
	char* FileName = "0.dat";
	char outname[20];
	FILE* fp;
	bool ExitCount = 0;
	if ((fp = fopen(FileName, "r")) == NULL) {
		printf("file open error\n");
		exit(EXIT_FAILURE);
	}
	unsigned long i = 0;
	unsigned long OutLimitMane = 0;
	i = 0;

	//このwhileループ内にてまず内部ソートを開始する
	//その際，OutFileLimit（20行目）に定められたレコード毎に一時ファイルを作成する
	while (1) {
		char** input_array;
		input_array = (char**)malloc(sizeof(char*) * OutFileLimit * 100);
		for (i = 0; i < OutFileLimit; i++) {
			input_array[i] = (char*)malloc(sizeof(char) * 100);
		}
		for (i = 0; i < OutFileLimit; i++) {
			//fgetsにてinput_array配列内に文字をファイルから入力する
			//同時に終端文字が出現した際にbool型のExitCountの値を1にしてfor文から脱出する
			if (fgets(input_array[i], 100, fp) == NULL) {
				ExitCount = 1;
				break;
			}
			//printf("%s\n", input_array[i]);
		}
		//bool型のExitCountが1の時，while文（内部ソート処理）から脱出する
		if (ExitCount) {
			break;
		}
		FILE* outfile;
		if (_mkdir("dat0") == 0) {
			printf("new folder\n");
		}
		sprintf(outname, "%s\\%d.dat", "dat0", OutLimitMane);
		outfile = fopen(outname, "w");
		char** aa = input_array - 1;
		//input_array配列内をソートする関数に配列を渡す
		sortQuickandMerge(1, OutFileLimit, aa);

		//一時ファイルにソートした配列のデータを1行づつ書き込む
		for (i = 0; i < OutFileLimit; i++) {
			//printf("%d : %s", i + 1, input_array[i]);
			fprintf(outfile, "%s", input_array[i]);
		}
		fclose(outfile);
		for (i = 0; i < OutFileLimit; i++) {
			free(input_array[i]);
		}
		free(input_array);
		printf("%d:End\n", OutLimitMane);
		//一時ファイル数を記録する
		OutLimitMane++;
	}
	fclose(fp);
	/*↑一時ファイル保存まで↑*/
	/*↓それらをマージしていく↓*/
	unsigned int foldroop = OutLimitMane;
	int tmp = OutLimitMane;
	//マージした際に何回ループするかを計算するfor文
	for (i = 0; OutLimitMane != 1; i++) {
		OutLimitMane = (OutLimitMane + 1) / 2;
		foldroop = i;
	}
	foldroop++;


	int q;
	unsigned long p;

	unsigned long Main_name;

	char removefile[20];

	//スレッド作成文
	//今回は12スレッド
	//マージされた一時ファイルが1つになるまでループする
	for (p = 0; p < foldroop; p++) {
		//一時ファイルをすべてマージするまでループする
		for (q = 0; q < tmp;) {
			HANDLE ThreadList[NUM_THREADS];
			struct Merge_info parlist[NUM_THREADS];
			for (i = 0; i < NUM_THREADS; i++) {
				if (i == 0) {
					parlist[i].name = q;
					parlist[i].fold = p;
				}
				else {
					parlist[i].name = parlist[i - 1].name + 2;
					parlist[i].fold = parlist[i - 1].fold;
				}
			}
			for (i = 0; i < NUM_THREADS; i++) {
				ThreadList[i] = (HANDLE)_beginthreadex(
					NULL,
					0,
					&Out_merge_ThreadEntry,
					(void*)&parlist[i],
					0,
					NULL
				);
			}
			Main_name = parlist[NUM_THREADS - 1].name + 2;
			Outmerge(Main_name, p);

			WaitForMultipleObjects(NUM_THREADS, ThreadList, TRUE, INFINITE);

			for (i = 0; i < NUM_THREADS; i++) {
				CloseHandle(ThreadList[i]);
			}
			q = Main_name + 2;
		}
		for (i = 0;; i++) {
			sprintf(removefile, "dat%d\\%d.dat", p, i);
			if (remove(removefile) != 0) {
				break;
			}
		}
		tmp = (tmp + 1) / 2;
	}

	char dir[20];
	for (i = 0; i < p; i++) {
		sprintf(dir, "dat%d", i);
		if (_rmdir(dir) == 0) {
			printf("delete\n");
		}
	}

	printf("\n\n\nソーティング完了しました\n");
	return 0;
}