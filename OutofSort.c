#include<stdio.h>
#include<stdlib.h>
#include <string.h>
#include<stdbool.h>
#pragma warning(disable:4996)

#define OutFileLimit 100000

#include<time.h>
#include<Windows.h>
#include<process.h>

#pragma comment(lib, "winmm.lib")

#define SWAP(a, b) (tmp) = (a);(a) = (b);(b) = (tmp);

#define M 32
#define NSTACK 50

struct thread_info {
	char **arr;
	unsigned long l;
	unsigned long r;
};

void sort(unsigned long left, unsigned long right, char** arr) {
	unsigned long i, j, k;
	unsigned long l = left;
	unsigned long r = right;
	int* stack, nstack = 0;
	char *a, *tmp;
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

void merge(unsigned long p, unsigned long q, unsigned long r, char **arr) {
	int i, k, j;
	int n1 = q - p + 1, n2 = r - q;
	char **aleft = (char**)malloc(sizeof(char) * n1 * 100);
	char **aright = (char**)malloc(sizeof(char) * n2 * 100);
	for (i = 0; i < n1; i++) {
		aleft[i] = malloc(sizeof(char) * 100);
	}
	for (i = 0; i < n2; i++) {
		aright[i] = malloc(sizeof(char) * 100);
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
	free(aleft);
	free(aright);
}

unsigned __stdcall qsortThreadEntry(void *arg) {
	struct thread_info *context = (struct thread_info *)arg;
	char **arr = context->arr;
	sort(context->l, context->r, arr);
	return 0;
}

unsigned __stdcall mergeThreadEntry(void *arg) {
	struct thread_info *context = (struct thread_info *)arg;
	char **arr = context->arr;
	merge(context->l, (context->l + context->r) >> 1, context->r, arr);
	return 0;
}

void sortQuickandMerge(unsigned long left, unsigned long right, char **arr) {
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
		(void*)&parLeft1,
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
		(void*)&parLeft2,
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
		(void*)&parRight1,
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
		(void*)&parLeftmerge,
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

	/*1分の1*/
	merge(l, k, r, arr);
	/*1分の1　end*/

	printf("\n\n");
}









int main() {
	char *FileName = "Testof10000000.dat";
	char outname[20];
	FILE *fp;
	bool ExitCount = 0;
	if ((fp = fopen(FileName, "r")) == NULL) {
		printf("file open error\n");
		exit(EXIT_FAILURE);
	}
	unsigned long i;
	unsigned long OutLimitMane = 0;
	i = 0;
	while (1) {
		char **input_array;
		input_array = malloc(sizeof(char *) * OutFileLimit * 100);

		for (i = 0; i < OutFileLimit; i++) {
			input_array[i] = malloc(sizeof(char) * 100);
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
		FILE *outfile;
		sprintf(outname, "%d.dat", OutLimitMane);
		outfile = fopen(outname, "w");
		char **aa = input_array - 1;
		sortQuickandMerge(1, OutFileLimit, aa);

		for (i = 0; i < OutFileLimit; i++) {
			//printf("%d : %s", i + 1, input_array[i]);
			fprintf(outfile, "%s", input_array[i]);
		}
		fclose(outfile);
		free(input_array);
		printf("%d:End\n", OutLimitMane);
		OutLimitMane++;
	}

	fclose(fp);
}