<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesVc extends Command
{
    protected $signature   = 'import:sales-vc';
    protected $description = 'VC 売上 CSV を csv_vc へ取込む';

    /* ---------- ヘルパ ---------- */

    /** class.php と同一：Y/m/d h:m:00 で固定フォーマット化 */
private static function toDate(?string $v): ?string
{
    return ($v !== '−')
        ? @date('Y/m/d h:m:00', strtotime($v))
        : null;
}

    /** 金額文字列 → 整数（円・税抜き） */
    private static function toInt(?string $v): int
    {
        return ($v === null || $v === '') ? 0 : (int)str_replace(',', '', $v);
    }

    /* ---------- 取込 ---------- */
    public function handle(): int
    {
        $srcDir    = storage_path('app/csv/Sales');
        $csvFiles  = glob("$srcDir/*vc*_sales.csv");          // VC 伝統の命名
        $headers   = ['クリック日'];                          // ヘッダー行判定

        foreach ($csvFiles as $csv) {
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(fn($r) => empty(array_filter($r)) || in_array($r[0] ?? '', $headers, true))
                ->each(function (array $r) {
                    DB::table('csv_vc')->insert([
                        'click_time'      => self::toDate($r[0]  ?? null),  // クリック日
                        'occur_time'      => self::toDate($r[1]  ?? null),  // 注文日
                        'fix_time'        => self::toDate($r[2]  ?? null),  // 処理日
                        'order_id'        => $r[3]   ?? null,
                        'ad_owner'        => $r[4]   ?? null,
                        'pg_name'         => ($r[5] ?? '') . ($r[16] ?? ''), // プログラム＋サイト名
                        'param_1'         => $r[6]   ?? null,
                        'sales'           => self::toInt($r[8]  ?? null),
                        'reward'          => self::toInt($r[9]  ?? null),
                        'statement'       => $r[12]  ?? null,
                        'approval_period' => $r[13]  ?? null,
                        'referer'         => $r[14]  ?? null,
                        'device'          => $r[15]  ?? null,
                        'site_name'       => $r[16]  ?? null,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('VC 売上データ取込完了');
        return Command::SUCCESS;
    }
}
