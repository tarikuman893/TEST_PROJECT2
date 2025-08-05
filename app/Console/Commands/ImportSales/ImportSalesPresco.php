<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesPresco extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:sales-presco';

    /** 説明 */
    protected $description = 'Presco 売上 CSV を csv_presco へ取込む';

    /* ---------- ヘルパ ---------- */
    /** 文字列日付 → Y-m-d H:i:s / null */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') return null;

        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    /** 金額文字列（カンマ・小数点込）→ 整数（円） */
    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    /* ---------- 取込 ---------- */
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Sales');
        $csvPattern = "$sourceDir/*presco*.csv";
        $headers    = ['ID'];                      // ヘッダー行判定用（1列目）

        foreach (glob($csvPattern) as $csv) {
            /* SJIS → UTF-8 変換（BOM 保護） */
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    $first = $row[0] ?? '';

                    // ①空行 ②ヘッダー行 ③order_id が数値でない行 は取り込まない
                    return (
                        (is_array($row) && count(array_filter($row, fn($v) => $v !== '')) === 0) ||
                        in_array($first, $headers, true) ||
                        !ctype_digit($first)
                    );
                })
                ->each(function (array $row) {
                    DB::table('csv_presco')->insert([
                        'order_id'   => (int) $row[0],                     // ID
                        'afid'       => $row[1]   ?? null,                 // AFID
                        'click_time' => self::toDate($row[2]  ?? null),    // クリック日時
                        'occur_time' => self::toDate($row[3]  ?? null),    // 成果発生日時
                        'fix_time'   => self::toDate($row[4]  ?? null),    // 成果判定日時
                        'site_name'  => $row[5]   ?? null,                 // 媒体名
                        'pg_name'    => $row[6]   ?? null,                 // 広告名
                        'lpid'       => $row[7]   ?? null,                 // 広告LPID
                        'lpname'     => $row[8]   ?? null,                 // 広告LP名
                        'ad_id'      => $row[9]   ?? null,                 // 広告素材ID
                        'u_id'       => $row[10]  ?? null,                 // UID
                        'device'     => $row[11]  ?? null,                 // OS
                        'referer'    => $row[12]  ?? null,                 // リファラ
                        'param_1'    => $row[13]  ?? null,                 // AFAD_PARAM_1
                        'param_2'    => $row[14]  ?? null,                 // AFAD_PARAM_2
                        'param_3'    => $row[15]  ?? null,                 // AFAD_PARAM_3
                        'sales'      => self::toInt($row[16] ?? null),     // 購入金額
                        'reward'     => self::toInt($row[17] ?? null),     // 成果報酬単価
                        'statement'  => $row[18]  ?? null,                 // ステータス
                    ]);
                });

            unlink($tmp);
        }

        $this->info('Presco 売上データ取込完了');
        return Command::SUCCESS;
    }
}
