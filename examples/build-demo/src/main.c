#include <stdio.h>

int one(void);
int two(void);
int three(void);
int four(void);

int main(void) {
  printf("%d\n", one() + two() + three() + four());
  return 0;
}
