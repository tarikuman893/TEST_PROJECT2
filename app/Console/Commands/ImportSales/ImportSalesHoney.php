<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesHoney extends Command
{
    /** artisan コマンド名 */
    protected $signature   = 'import:sales-honey';

    /** 説明 */
    protected $description = 'Honey 売上 CSV を csv_honey へ取込む';

    /** 文字列日付 → Y-m-d H:i:s / null */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    /** 取込処理 */
    public function handle(): int
    {
        /* ---------- 対象ファイル ---------- */
        $sourceDir  = storage_path('app/csv/Sales');
        // 例: honey_sales.csv など（honeycomb_〜 は対象外）
        $csvPattern = "$sourceDir/*honey_sales.csv";
        $headers    = ['成果ID'];   // ヘッダー行判定

        foreach (glob($csvPattern) as $csv) {

            /* ---------- エンコーディング変換 ---------- */
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            /* ---------- 取込 ---------- */
            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    // 空行 / ヘッダー行 / 成果ID が空 を除外
                    $first = $row[0] ?? '';
                    return (
                        (is_array($row) && count(array_filter($row, fn($v) => $v !== '')) === 0) ||
                        in_array($first, $headers, true) ||
                        $first === ''
                    );
                })
                ->each(function (array $row) {
                    DB::table('csv_honey')->insert([
                        'order_id'        => $row[0]  ?? null,
                        'click_time'      => self::toDate($row[1]  ?? null),
                        'occur_time'      => self::toDate($row[2]  ?? null),
                        'fix_time'        => self::toDate($row[3]  ?? null),
                        'pg_name'         => $row[4]  ?? null,
                        'pg_flg'          => $row[5]  ?? null,
                        'campaign'        => $row[6]  ?? null,
                        'lpid'            => $row[7]  ?? null,
                        'lp'              => $row[8]  ?? null,
                        'lpurl'           => $row[9]  ?? null,
                        'device'          => $row[10] ?? null,
                        'os'              => $row[11] ?? null,
                        'statement'       => $row[12] ?? null,
                        'referer'         => $row[13] ?? null,
                        'tracking_param'  => $row[14] ?? null,
                        'reward'          => 0,           // 本ファイルには報酬列なし
                    ]);
                });

            unlink($tmp);
        }

        $this->info('Honey 売上データ取込完了');
        return Command::SUCCESS;
    }
}
