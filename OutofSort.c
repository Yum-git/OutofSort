#include<stdio.h>
#include<stdlib.h>
#include <string.h>
#include<stdbool.h>
#pragma warning(disable:4996)
#define OutFileLimit 100000
#include<time.h>
#include<Windows.h>
#include<process.h>
#include<direct.h>
#pragma comment(lib, "winmm.lib")
#define SWAP(a, b) (tmp) = (a);(a) = (b);(b) = (tmp);
#define M 32
#define NSTACK 50
int Foldernumber = 1;
int Bool_1 = 0;
void Outmerge(unsigned long i, unsigned long q);
struct Merge_info {
	unsigned long name;
	unsigned long fold;
};
unsigned __stdcall Out_merge_ThreadEntry(void* arg) {
	struct Merge_info* context = (struct Merge_info*)arg;
	Outmerge(context->name, context->fold);
	return 0;
}
void Outmerge(unsigned long n, unsigned long f) {
	unsigned long name = n;
	unsigned long Foldname = f;
	char merge_first[20];
	char merge_second[20];
	char merge_result[20];
	char Foldername[20];
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
struct thread_info {
	char** arr;
	unsigned long l;
	unsigned long r;
};
void sort(unsigned long left, unsigned long right, char** arr) {
	unsigned long i, j, k;
	unsigned long l = left;
	unsigned long r = right;
	int* stack, nstack = 0;
	char* a, *tmp;
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
	free(stack + 1);
}
void merge(unsigned long p, unsigned long q, unsigned long r, char** arr) {
	printf("Merge in\n");
	int i, k, j;
	int n1 = q - p + 1, n2 = r - q;
	char** aleft = (char**)malloc(sizeof(char) * n1 * 100);
	char** aright = (char**)malloc(sizeof(char) * n2 * 100);
	for (i = 0; i < n1; i++) {
		aleft[i] = (char*)malloc(sizeof(char) * 100);
	}
	for (i = 0; i < n2; i++) {
		aright[i] = (char*)malloc(sizeof(char) * 100);
	}
	for (i = 0; i < n1; i++) {
		aleft[i] = arr[p + i];
	}
	for (i = 0; i < n2; i++) {
		aright[i] = arr[q + 1 + i];
	}
	for (k = i = j = 0; k < n1 + n2; k++) {
		if (i >= n1 || (j < n2 && memcmp(aleft[i], aright[j], 10) > 0)) {
			arr[k + p] = aright[j++];
		}
		else {
			arr[k + p] = aleft[i++];
		}
	}
	for (i = 0; i < n1; i++) {
		free(aleft[i]);
	}
	for (i = 0; i < n2; i++) {
		free(aright[i]);
	}
	free(aleft);
	free(aright);
	printf("Merge Out\n");
}
unsigned __stdcall qsortThreadEntry(void* arg) {
	struct thread_info* context = (struct thread_info*)arg;
	char** arr = context->arr;
	sort(context->l, context->r, arr);
	return 0;
}
unsigned __stdcall mergeThreadEntry(void* arg) {
	struct thread_info* context = (struct thread_info*)arg;
	char** arr = context->arr;
	merge(context->l, (context->l + context->r) >> 1, context->r, arr);
	return 0;
}
void sortQuickandMerge(unsigned long left, unsigned long right, char** arr) {
	unsigned long k;
	unsigned long l = left;
	unsigned long r = right;
	unsigned long half1, half2;
	HANDLE LeftMainThread, LeftSubThread, RightSubThread;
	HANDLE LeftThread_merge;
	struct thread_info parLeft1, parLeft2, parRight1;
	struct thread_info parLeftmerge;
	if (left >= right) {
		exit(0);
	}
	sort(l, r, arr);
	return;





	/*下は現在未使用*/
	k = (l + r) >> 1;
	half1 = (l + k) >> 1;
	half2 = (k + r) >> 1;
	/*4分の1*/
	parLeft1.l = l;
	parLeft1.r = half1;
	parLeft1.arr = arr;
	LeftMainThread = (HANDLE)_beginthreadex(
		NULL,
		0,
		&qsortThreadEntry,
		(void*)& parLeft1,
		0,
		NULL
	);
	if (LeftMainThread == NULL) {
		printf("_beginthreadex() failed, error: %d\n", GetLastError());
		return;
	}
	/*4分の1　end*/
	/*4分の2*/
	parLeft2.l = half1 + 1;
	parLeft2.r = k;
	parLeft2.arr = arr;
	LeftSubThread = (HANDLE)_beginthreadex(
		NULL,
		0,
		&qsortThreadEntry,
		(void*)& parLeft2,
		0,
		NULL
	);
	if (LeftSubThread == NULL) {
		printf("_beginthreadex() failed, error: %d\n", GetLastError());
		return;
	}
	/*4分の2 end*/
	/*4分の3*/
	parRight1.l = k + 1;
	parRight1.r = half2;
	parRight1.arr = arr;
	RightSubThread = (HANDLE)_beginthreadex(
		NULL,
		0,
		&qsortThreadEntry,
		(void*)& parRight1,
		0,
		NULL
	);
	if (RightSubThread == NULL) {
		printf("_beginthreadex() failed, error: %d\n", GetLastError());
		return;
	}
	/*4分の3 end*/
	/*4分の4*/
	sort(half2 + 1, r, arr);
	/*4分の4 end*/
	WaitForSingleObject(LeftMainThread, INFINITE);
	WaitForSingleObject(LeftSubThread, INFINITE);
	WaitForSingleObject(RightSubThread, INFINITE);
	CloseHandle(LeftMainThread);
	CloseHandle(LeftSubThread);
	CloseHandle(RightSubThread);
	/*2分の1*/
	parLeftmerge.l = l;
	parLeftmerge.r = k;
	parLeftmerge.arr = arr;
	LeftThread_merge = (HANDLE)_beginthreadex(
		NULL,
		0,
		&mergeThreadEntry,
		(void*)& parLeftmerge,
		0,
		NULL
	);
	if (LeftThread_merge == NULL) {
		printf("_beginthreadex() failed, error: %d\n", GetLastError());
		return;
	}
	/*2分の1 end*/
	/*2分の2*/
	merge(k + 1, (k + 1 + r) >> 1, r, arr);
	/*2分の2 end*/
	WaitForSingleObject(LeftThread_merge, INFINITE);
	CloseHandle(LeftThread_merge);
	printf("merge first in\n");
	/*1分の1*/
	merge(l, k, r, arr);
	/*1分の1　end*/
	printf("\n\n");







	/*上は現在未使用*/

}
int main() {
	char* FileName = "160000000.dat";
	char outname[20];
	FILE* fp;
	bool ExitCount = 0;
	if ((fp = fopen(FileName, "r")) == NULL) {
		printf("file open error\n");
		exit(EXIT_FAILURE);
	}
	unsigned long i;
	unsigned long OutLimitMane = 0;
	i = 0;
	while (1) {
		char** input_array;
		input_array = (char**)malloc(sizeof(char*) * OutFileLimit * 100);
		for (i = 0; i < OutFileLimit; i++) {
			input_array[i] = (char*)malloc(sizeof(char) * 100);
		}
		for (i = 0; i < OutFileLimit; i++) {
			if (fgets(input_array[i], 100, fp) == NULL) {
				ExitCount = 1;
				break;
			}
			//printf("%s\n", input_array[i]);
		}
		if (ExitCount) {
			break;
		}
		FILE* outfile;
		sprintf(outname, "%s\\%d.dat", "dat0", OutLimitMane);
		outfile = fopen(outname, "w");
		char** aa = input_array - 1;
		sortQuickandMerge(1, OutFileLimit, aa);
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
		OutLimitMane++;
	}
	fclose(fp);
	/*↑一時ファイル保存まで↑*/
	/*↓それらをマージしていく↓*/
	unsigned int foldroop = OutLimitMane;
	int tmp = OutLimitMane;
	for (i = 0; OutLimitMane != 1; i++) {
		OutLimitMane = (OutLimitMane + 1) / 2;
		foldroop = i;
	}
	foldroop++;


	int q;
	unsigned long p;
	unsigned long par4_name;

	char removefile[20];
	for (p = 0; p < foldroop; p++) {
		for (q = 0; q < tmp;) {
			HANDLE SecondThread, ThirdThread, FoursThread;
			struct Merge_info par1, par2, par3;
			par1.name = q;
			par1.fold = p;
			par2.name = par1.name + 2;
			par2.fold = p;
			par3.name = par2.name + 2;
			par3.fold = p;
			printf("%d, %d ", par1.name, par1.name + 1);
			printf("%d, %d ", par2.name, par2.name + 1);
			printf("%d, %d ", par3.name, par3.name + 1);
			SecondThread = (HANDLE)_beginthreadex(
				NULL,
				0,
				&Out_merge_ThreadEntry,
				(void*)& par1,
				0,
				NULL
			);
			ThirdThread = (HANDLE)_beginthreadex(
				NULL,
				0,
				&Out_merge_ThreadEntry,
				(void*)& par2,
				0,
				NULL
			);
			FoursThread = (HANDLE)_beginthreadex(
				NULL,
				0,
				&Out_merge_ThreadEntry,
				(void*)& par3,
				0,
				NULL
			);
			par4_name = par3.name + 2;
			Outmerge(par4_name, p);
			WaitForSingleObject(SecondThread, INFINITE);
			WaitForSingleObject(ThirdThread, INFINITE);
			WaitForSingleObject(FoursThread, INFINITE);
			CloseHandle(SecondThread);
			CloseHandle(ThirdThread);
			CloseHandle(FoursThread);
			q = par4_name + 2;
		}
		for (i = 0;; i++) {
			sprintf(removefile, "dat%d\\%d.dat", p, i);
			if (remove(removefile) != 0) {
				break;
			}
		}
		tmp = (tmp + 1) / 2;
	}
	return 0;
}
